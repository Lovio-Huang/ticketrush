package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import com.yitaihuang.ticketrush.AbstractIntegrationTest;
import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.event.Event;
import com.yitaihuang.ticketrush.order.OrderFailureService;
import com.yitaihuang.ticketrush.order.OrderRequested;
import com.yitaihuang.ticketrush.order.OrderService;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * The sale ends when the event starts, but what was accepted before that keeps
 * being processed: sent to Kafka, stored, released when cancelled.
 */
class SaleCutoffIntegrationTest extends AbstractIntegrationTest {

	@Autowired StockGateway stock;

	@Autowired OrderOutboxRelay relay;

	@Autowired StockReconciler reconciler;

	@Autowired JdbcTemplate jdbc;

	@Autowired CacheManager caches;

	@Autowired OrderService orderService;

	@Autowired OrderFailureService failures;

	@Autowired StockRecovery recovery;

	@Autowired TicketrushProperties properties;

	@Autowired MeterRegistry meters;

	@Autowired TransactionTemplate transactions;

	@Test
	void afterTheEventStartsNewReservationsAreRefusedButARetryGetsItsOrder() {
		long eventId = createEvent(10, 4);
		String alice = String.valueOf(userId("cutoff-alice"));
		String bob = String.valueOf(userId("cutoff-bob"));
		String orderId = reservations.reserve(eventId, alice, 1, "k1").orderId();
		await().atMost(Duration.ofSeconds(15)).until(() -> orders.existsById(orderId));

		startEvent(eventId, Instant.now().minusSeconds(1));

		assertThatThrownBy(() -> reservations.reserve(eventId, bob, 1, "k1"))
			.hasFieldOrPropertyWithValue("code", "SALE_ENDED");
		assertThatThrownBy(() -> reservations.reserve(eventId, alice, 1, "k2"))
			.hasFieldOrPropertyWithValue("code", "SALE_ENDED");
		ReservationService.Result retry = reservations.reserve(eventId, alice, 1, "k1");
		assertThat(retry.orderId()).isEqualTo(orderId);
		assertThat(retry.replayed()).isTrue();

		assertThat(orders.findByEventId(eventId)).hasSize(1);
		assertThat(stock.available(eventId)).hasValue(9);
		awaitStoresAgree(eventId);
	}

	@Test
	void aRequestThatPausedPastTheStartIsRefusedByRedisItself() {
		long eventId = createEvent(10, 4);
		String user = String.valueOf(userId("paused"));
		String accepted = reserveInRedis(eventId, user, 1);
		// Both passed the app's check while the sale was open, then reached Redis after it ended.
		Instant ended = Instant.now().minusMillis(1);
		String late = UUID.randomUUID().toString();
		String message = json.writeValueAsString(new OrderRequested(late, eventId, user, 1, 2500, "k2", Instant.now()));

		assertThat(stock.reserve(eventId, user, 1, 4, "k2", late, message, ended).outcome())
			.isEqualTo(StockGateway.Outcome.SALE_ENDED);
		assertThat(stock.reserve(eventId, user, 1, 4, "k1", late, message, ended))
			.isEqualTo(new StockGateway.Reservation(StockGateway.Outcome.DUPLICATE, accepted));
		assertThat(stock.available(eventId)).hasValue(9);
		assertThat(stock.outboxSize(eventId)).isEqualTo(1); // only the accepted one
	}

	@Test
	void anOrderAcceptedBeforeTheStartButNeverConfirmedIsStillDeliveredAfterIt() {
		long eventId = createEvent(10, 4);
		// Accepted while on sale; Kafka never confirmed it, so it sits in the outbox.
		String orderId = reserveInRedis(eventId, String.valueOf(userId("late-relay")), 2);

		startEvent(eventId, Instant.now().minusSeconds(1));
		relay.run();

		await().atMost(Duration.ofSeconds(15)).until(() -> orders.existsById(orderId));
		awaitStoresAgree(eventId);
	}

	@Test
	void aCancellationAfterTheStartIsStillReleasedInRedis() {
		long eventId = createEvent(10, 4);
		String user = String.valueOf(userId("late-cancel"));
		String orderId = reserveAndAwaitOrder(eventId, user, 2);
		startEvent(eventId, Instant.now().minusSeconds(1));
		// Cancelled in MySQL; the release in Redis after the commit was lost (e.g. Redis briefly unreachable).
		jdbc.update("update orders set status = 'CANCELLED' where id = ?", orderId);
		jdbc.update("update events set available_tickets = available_tickets + 2 where id = ?", eventId);

		reconciler.run();

		assertThat(stock.available(eventId)).hasValue(10);
		assertStoresAgree(eventId);
	}

	@Test
	void aSettledEventIsReopenedByAnExpiryAndItsTicketsAreStillReleased() {
		long eventId = createEvent(10, 4);
		String user = String.valueOf(userId("settle-expiry"));
		String orderId = reserveAndAwaitOrder(eventId, user, 2);
		startEvent(eventId, Instant.now().minus(2, ChronoUnit.HOURS));

		reconciler.run();
		assertThat(upkeepIds()).as("nothing to do right now, so settled").doesNotContain(eventId);

		// Its payment window runs out; the expiry job is not limited to events on sale.
		jdbc.update("update orders set expires_at = ? where id = ?", utc(Instant.now().minusSeconds(1)), orderId);
		assertThat(orderService.expireOverdue(100)).isPositive();

		assertThat(upkeepIds()).as("the expiry reopened it").contains(eventId);
		assertThat(stock.available(eventId)).hasValue(10);
		assertStoresAgree(eventId);
	}

	@Test
	void aDeadLetterForASettledEventReopensIt() {
		long eventId = createEvent(10, 4);
		startEvent(eventId, Instant.now().minus(2, ChronoUnit.HOURS));
		// The first run creates the held counter no reservation has made yet; a run with nothing to fix settles.
		await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
			reconciler.run();
			assertThat(upkeepIds()).doesNotContain(eventId);
		});

		// A message that reached Kafka before the start, dead-lettered after the event was settled.
		String message = json.writeValueAsString(new OrderRequested(UUID.randomUUID().toString(), eventId,
				String.valueOf(userId("settle-dlt")), 1, 2500, "k1", Instant.now()));
		failures.record(message, "boom", "test-settle-dlt-" + eventId + "@1");

		assertThat(upkeepIds()).contains(eventId);
	}

	@Test
	void afterTheStartALostRedisStateIsStillRebuilt() {
		long eventId = createEvent(10, 4);
		reserveAndAwaitOrder(eventId, String.valueOf(userId("late-rebuild")), 2);
		startEvent(eventId, Instant.now().minusSeconds(1));
		wipeRedis(eventId);
		quietPeriodOver(eventId);

		reconciler.run();

		assertThat(stock.available(eventId)).hasValue(8);
		assertStoresAgree(eventId);
	}

	@Test
	void aCancellationThatLandsWhileTheEventIsBeingSettledIsStillReleased() {
		long eventId = createEvent(10, 4);
		String user = String.valueOf(userId("settle-race"));
		String orderId = reserveAndAwaitOrder(eventId, user, 2);
		orderService.pay(reserveAndAwaitOrder(eventId, user, 1), user); // keeps the event from being trivially empty
		startEvent(eventId, Instant.now().minus(2, ChronoUnit.HOURS));

		// The reconciler has found nothing to do and is checking whether the event can be settled. Right
		// then the order is cancelled (MySQL commits, through the same writes OrderService.cancel makes),
		// and the release in Redis after the commit is lost.
		StockGateway checking = spy(stock);
		AtomicBoolean cancelled = new AtomicBoolean();
		doAnswer(call -> {
			if (call.getArgument(0).equals(eventId) && cancelled.compareAndSet(false, true)) {
				transactions.executeWithoutResult(tx -> {
					jdbc.update("update orders set status = 'CANCELLED' where id = ?", orderId);
					eventRepository.incrementAvailable(eventId, 2);
				});
			}
			return call.callRealMethod();
		}).when(checking).outboxSize(anyLong());
		StockReconciler racing = reconcilerUsing(checking);
		await().atMost(Duration.ofSeconds(15)).until(() -> {
			racing.run();
			return cancelled.get();
		});

		reconciler.run(); // later runs must still see the event
		reconciler.run();
		assertThat(stock.available(eventId)).as("the cancelled order's tickets").hasValue(9);
		assertStoresAgree(eventId);
	}

	private List<Long> upkeepIds() {
		return eventRepository.findNeedingUpkeep(Instant.now()).stream().map(Event::getId).toList();
	}

	private StockReconciler reconcilerUsing(StockGateway gateway) {
		return new StockReconciler(eventRepository, orders, gateway, recovery, properties, meters, Clock.systemUTC());
	}

	/** Moves the event's start (and so the end of its sale) to {@code startsAt}. */
	private void startEvent(long eventId, Instant startsAt) {
		jdbc.update("update events set starts_at = ? where id = ?", utc(startsAt), eventId);
		caches.getCache("events").evict(eventId);
	}

	/** Instants are stored as UTC wall-clock time. */
	private static LocalDateTime utc(Instant instant) {
		return instant.atOffset(ZoneOffset.UTC).toLocalDateTime();
	}

	/** Reserved in Redis while the sale was open, never published. */
	private String reserveInRedis(long eventId, String user, int quantity) {
		String orderId = UUID.randomUUID().toString();
		String message = json.writeValueAsString(
				new OrderRequested(orderId, eventId, user, quantity, quantity * 2500, "k1", Instant.now()));
		stock.reserve(eventId, user, quantity, 4, "k1", orderId, message, SALE_OPEN);
		return orderId;
	}

}
