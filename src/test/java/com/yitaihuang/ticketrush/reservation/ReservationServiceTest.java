package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpStatus;

import com.yitaihuang.ticketrush.common.ApiException;
import com.yitaihuang.ticketrush.event.EventCatalog;
import com.yitaihuang.ticketrush.event.EventSnapshot;
import com.yitaihuang.ticketrush.order.Order;
import com.yitaihuang.ticketrush.order.OrderRepository;
import com.yitaihuang.ticketrush.order.OrderRequested;
import com.yitaihuang.ticketrush.order.PendingOrderTracker;
import com.yitaihuang.ticketrush.reservation.OrderRequestPublisher.Delivery;
import com.yitaihuang.ticketrush.reservation.StockGateway.Outcome;
import com.yitaihuang.ticketrush.reservation.StockGateway.Reservation;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import tools.jackson.databind.json.JsonMapper;

class ReservationServiceTest {

	private static final Instant NOW = Instant.parse("2027-03-01T12:00:00Z");

	private static final EventSnapshot EVENT = new EventSnapshot(1L, "Concert", "Arena", NOW.plusSeconds(86_400),
			NOW.minusSeconds(60), 100, 2_500, 4);

	private final EventCatalog catalog = mock();

	private final StockGateway stock = mock();

	private final StockRecovery recovery = mock();

	private final RateLimiter rateLimiter = mock();

	private final OrderRequestPublisher publisher = mock();

	private final PendingOrderTracker pendingOrders = mock();

	private final OrderRepository orders = mock();

	private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

	private ReservationService service;

	@BeforeEach
	void setUp() {
		service = new ReservationService(catalog, stock, recovery, rateLimiter, publisher, pendingOrders, orders,
				new JsonMapper(), meters, Clock.fixed(NOW, ZoneOffset.UTC));
		when(publisher.publish(anyLong(), anyString(), anyString())).thenReturn(Delivery.ACKED);
		when(rateLimiter.tryAcquire(anyString())).thenReturn(true);
		when(recovery.redisTrusted()).thenReturn(true);
		when(catalog.get(1L)).thenReturn(EVENT);
	}

	@Test
	void untilRedisIsTrustedReservationsAreRefusedWithoutTouchingIt() {
		when(recovery.redisTrusted()).thenReturn(false);

		assertThatThrownBy(() -> service.reserve(1L, "alice", 1, "key-1"))
			.hasFieldOrPropertyWithValue("code", "STOCK_RECOVERING");
		verifyNoInteractions(stock, publisher);
	}

	@Test
	void acceptedReservationIsPublishedWithTheTotalPrice() {
		givenRedisAnswers(Outcome.RESERVED);

		ReservationService.Result result = service.reserve(1L, "alice", 2, "key-1");

		assertThat(result.replayed()).isFalse();
		ArgumentCaptor<String> message = ArgumentCaptor.captor();
		verify(publisher).publish(eq(1L), eq(result.orderId()), message.capture());
		assertThat(new JsonMapper().readValue(message.getValue(), OrderRequested.class))
			.isEqualTo(new OrderRequested(result.orderId(), 1L, "alice", 2, 5_000, "key-1", NOW));
		verify(stock).markPublished(1L, result.orderId());
		verify(pendingOrders).markProcessing(result.orderId(), "alice");
		assertThat(meters.counter("ticketrush.reservations", "outcome", "accepted").count()).isEqualTo(1);
	}

	@Test
	void theHighestSupportedPriceTimesTheLargestQuantityIsExact() {
		EventSnapshot priciest = new EventSnapshot(2L, "Gala", "Arena", NOW.plusSeconds(86_400), NOW.minusSeconds(60),
				100, 100_000_000, 10);
		when(catalog.get(2L)).thenReturn(priciest);
		givenRedisAnswers(Outcome.RESERVED);

		ReservationService.Result result = service.reserve(2L, "alice", 10, "key-1");

		ArgumentCaptor<String> message = ArgumentCaptor.captor();
		verify(publisher).publish(eq(2L), eq(result.orderId()), message.capture());
		assertThat(new JsonMapper().readValue(message.getValue(), OrderRequested.class).amountCents())
			.isEqualTo(1_000_000_000);
	}

	@Test
	void anAmountTooLargeToRepresentIsRefusedBeforeAnyTicketIsHeld() {
		// An event stored before prices were capped: two tickets overflow an int.
		EventSnapshot legacy = new EventSnapshot(3L, "Legacy", "Arena", NOW.plusSeconds(86_400), NOW.minusSeconds(60),
				100, Integer.MAX_VALUE, 4);
		when(catalog.get(3L)).thenReturn(legacy);

		assertThatThrownBy(() -> service.reserve(3L, "alice", 2, "key-1")).isInstanceOfSatisfying(ApiException.class,
				ex -> {
					assertThat(ex.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST);
					assertThat(ex.getCode()).isEqualTo("AMOUNT_OUT_OF_RANGE");
				});
		verifyNoInteractions(stock, publisher, pendingOrders);
	}

	@Test
	void theSaleClosesExactlyWhenTheEventStarts() {
		// startsAt == now: closed. One millisecond before: still open (acceptedReservationIsPublished...).
		EventSnapshot starting = new EventSnapshot(4L, "Now", "Arena", NOW, NOW.minusSeconds(3_600), 100, 2_500, 4);
		when(catalog.get(4L)).thenReturn(starting);
		givenRedisAnswers(Outcome.SALE_ENDED);

		assertThatThrownBy(() -> service.reserve(4L, "alice", 1, "key-1")).isInstanceOfSatisfying(ApiException.class,
				ex -> {
					assertThat(ex.getStatus()).isEqualTo(HttpStatus.CONFLICT);
					assertThat(ex.getCode()).isEqualTo("SALE_ENDED");
				});
		// The app's clock decided the sale is over: Redis only looks the key up, with an end in the past.
		verify(stock).reserve(eq(4L), anyString(), anyInt(), anyInt(), anyString(), anyString(), anyString(),
				eq(Instant.EPOCH));
		verifyNoInteractions(publisher);
	}

	@Test
	void oneMillisecondBeforeTheEventStartsTheSaleIsStillOpen() {
		EventSnapshot almost = new EventSnapshot(5L, "Almost", "Arena", NOW.plusMillis(1), NOW.minusSeconds(3_600), 100,
				2_500, 4);
		when(catalog.get(5L)).thenReturn(almost);
		givenRedisAnswers(Outcome.RESERVED);

		assertThat(service.reserve(5L, "alice", 1, "key-1").replayed()).isFalse();
	}

	@Test
	void afterTheEventStartsARetryOfAnAcceptedRequestStillGetsItsOrder() {
		EventSnapshot started = new EventSnapshot(6L, "Started", "Arena", NOW.minusSeconds(60), NOW.minusSeconds(3_600),
				100, 2_500, 4);
		when(catalog.get(6L)).thenReturn(started);
		givenRedisAnswers(Outcome.SALE_ENDED);
		when(stock.reserve(eq(6L), eq("alice"), eq(1), anyInt(), eq("key-1"), anyString(), anyString(),
				eq(Instant.EPOCH)))
			.thenReturn(new Reservation(Outcome.DUPLICATE, "order-1"));

		ReservationService.Result retry = service.reserve(6L, "alice", 1, "key-1");

		assertThat(retry.orderId()).isEqualTo("order-1");
		assertThat(retry.replayed()).isTrue();
		verifyNoInteractions(publisher);
		// A different key (or another user) is a new request: refused.
		assertThatThrownBy(() -> service.reserve(6L, "alice", 1, "key-2"))
			.hasFieldOrPropertyWithValue("code", "SALE_ENDED");
		assertThatThrownBy(() -> service.reserve(6L, "bob", 1, "key-1"))
			.hasFieldOrPropertyWithValue("code", "SALE_ENDED");
	}

	@Test
	void aKeyUsedForAnotherQuantityIsAConflictAndNothingIsSent() {
		givenRedisAnswers(Outcome.IDEMPOTENCY_CONFLICT);

		assertThatThrownBy(() -> service.reserve(1L, "alice", 2, "key-1")).isInstanceOfSatisfying(ApiException.class,
				ex -> {
					assertThat(ex.getStatus()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
					assertThat(ex.getCode()).isEqualTo("IDEMPOTENCY_CONFLICT");
				});
		verifyNoInteractions(publisher, pendingOrders);
	}

	@Test
	void aKeyRedisCantVerifyIsCheckedAgainstTheStoredOrder() {
		when(stock.reserve(anyLong(), anyString(), anyInt(), anyInt(), anyString(), anyString(), anyString(), any()))
			.thenReturn(new Reservation(Outcome.DUPLICATE_UNVERIFIED, "order-1"));
		Order stored = mock();
		when(stored.isSameRequestAs(1L, "alice", 2)).thenReturn(true);
		when(orders.findById("order-1")).thenReturn(Optional.of(stored));

		assertThat(service.reserve(1L, "alice", 2, "key-1")).isEqualTo(new ReservationService.Result("order-1", true));
		assertThatThrownBy(() -> service.reserve(1L, "alice", 3, "key-1"))
			.hasFieldOrPropertyWithValue("code", "IDEMPOTENCY_CONFLICT");
		when(orders.findById("order-1")).thenReturn(Optional.empty()); // nothing left to compare with: refused
		assertThatThrownBy(() -> service.reserve(1L, "alice", 2, "key-1"))
			.hasFieldOrPropertyWithValue("code", "IDEMPOTENCY_CONFLICT");
		verifyNoInteractions(publisher);
	}

	@Test
	void redisStoresTheSameMessageThatIsPublished() {
		givenRedisAnswers(Outcome.RESERVED);

		service.reserve(1L, "alice", 2, "key-1");

		ArgumentCaptor<String> stored = ArgumentCaptor.captor();
		ArgumentCaptor<String> published = ArgumentCaptor.captor();
		verify(stock).reserve(anyLong(), anyString(), anyInt(), anyInt(), anyString(), anyString(), stored.capture(), any());
		verify(publisher).publish(anyLong(), anyString(), published.capture());
		assertThat(published.getValue()).isEqualTo(stored.getValue());
	}

	@Test
	void duplicateRequestReturnsTheOriginalOrderWithoutPublishingAgain() {
		when(stock.reserve(eq(1L), eq("alice"), eq(1), eq(4), eq("key-1"), anyString(), anyString(), any()))
			.thenReturn(new Reservation(Outcome.DUPLICATE, "original-order"));

		ReservationService.Result result = service.reserve(1L, "alice", 1, "key-1");

		assertThat(result).isEqualTo(new ReservationService.Result("original-order", true));
		verifyNoInteractions(publisher);
		verify(pendingOrders).markProcessing("original-order", "alice");
	}

	@Test
	void ticketsAreReturnedWhenTheOrderDefinitelyWasNotSent() {
		givenRedisAnswers(Outcome.RESERVED);
		when(publisher.publish(anyLong(), anyString(), anyString())).thenReturn(Delivery.NOT_SENT);
		when(stock.releaseAndForget(anyLong(), anyString(), anyString(), anyString())).thenReturn(true);

		assertThatThrownBy(() -> service.reserve(1L, "alice", 2, "key-1")).isInstanceOf(ApiException.class)
			.hasFieldOrPropertyWithValue("status", HttpStatus.SERVICE_UNAVAILABLE);
		verify(stock).releaseAndForget(eq(1L), anyString(), eq("alice"), eq("key-1"));
		verify(pendingOrders, never()).markProcessing(anyString(), anyString());
	}

	@Test
	void aNotSentOrderTheRelayAlreadyTookIsAcceptedNotReleased() {
		givenRedisAnswers(Outcome.RESERVED);
		when(publisher.publish(anyLong(), anyString(), anyString())).thenReturn(Delivery.NOT_SENT);
		when(stock.releaseAndForget(anyLong(), anyString(), anyString(), anyString())).thenReturn(false);

		ReservationService.Result result = service.reserve(1L, "alice", 2, "key-1");

		assertThat(result.replayed()).isFalse();
		verify(pendingOrders).markProcessing(result.orderId(), "alice");
		assertThat(meters.counter("ticketrush.reservations", "outcome", "unconfirmed").count()).isEqualTo(1);
	}

	@Test
	void anUnconfirmedSendKeepsTheTicketsAndLeavesTheOrderToTheRelay() {
		givenRedisAnswers(Outcome.RESERVED);
		when(publisher.publish(anyLong(), anyString(), anyString())).thenReturn(Delivery.UNKNOWN);

		ReservationService.Result result = service.reserve(1L, "alice", 2, "key-1");

		assertThat(result.replayed()).isFalse();
		verify(stock, never()).releaseAndForget(anyLong(), anyString(), anyString(), anyString());
		verify(stock, never()).markPublished(anyLong(), anyString()); // stays in the outbox
		verify(pendingOrders).markProcessing(result.orderId(), "alice");
		assertThat(meters.counter("ticketrush.reservations", "outcome", "unconfirmed").count()).isEqualTo(1);
	}

	@Test
	void aBackloggedQueueRefusesNewReservations() {
		givenRedisAnswers(Outcome.QUEUE_BACKLOGGED);

		assertThatThrownBy(() -> service.reserve(1L, "alice", 1, "key-1"))
			.hasFieldOrPropertyWithValue("code", "ORDER_QUEUE_UNAVAILABLE");
		verifyNoInteractions(publisher);
	}

	@Test
	void soldOutIsReportedAsConflict() {
		givenRedisAnswers(Outcome.SOLD_OUT);

		assertThatThrownBy(() -> service.reserve(1L, "alice", 1, "key-1"))
			.hasFieldOrPropertyWithValue("code", "SOLD_OUT");
		verifyNoInteractions(publisher);
	}

	@Test
	void rateLimitedUsersNeverReachRedisStock() {
		when(rateLimiter.tryAcquire("bot")).thenReturn(false);

		assertThatThrownBy(() -> service.reserve(1L, "bot", 1, "key-1"))
			.hasFieldOrPropertyWithValue("status", HttpStatus.TOO_MANY_REQUESTS);
		verifyNoInteractions(stock, publisher);
	}

	@Test
	void requestsBeforeTheSaleOpensAreRejected() {
		when(catalog.get(2L)).thenReturn(new EventSnapshot(2L, "Later", "Arena", NOW.plusSeconds(86_400),
				NOW.plusSeconds(3_600), 100, 2_500, 4));

		assertThatThrownBy(() -> service.reserve(2L, "alice", 1, "key-1"))
			.hasFieldOrPropertyWithValue("code", "SALE_NOT_STARTED");
		verifyNoInteractions(stock);
	}

	@Test
	void missingRedisStateIsRebuiltAndTheReservationRetried() {
		when(recovery.rebuild(1L)).thenReturn(true);
		when(stock.reserve(anyLong(), anyString(), anyInt(), anyInt(), anyString(), anyString(), anyString(), any()))
			.thenReturn(new Reservation(Outcome.NOT_INITIALIZED, ""))
			.thenAnswer(inv -> new Reservation(Outcome.RESERVED, inv.getArgument(5)));

		service.reserve(1L, "alice", 1, "key-1");

		verify(publisher).publish(anyLong(), anyString(), anyString());
	}

	@Test
	void whileRedisCannotBeRebuiltYetReservationsAreRefused() {
		givenRedisAnswers(Outcome.NOT_INITIALIZED);
		when(recovery.rebuild(1L)).thenReturn(false);

		assertThatThrownBy(() -> service.reserve(1L, "alice", 1, "key-1"))
			.hasFieldOrPropertyWithValue("code", "STOCK_RECOVERING");
		verifyNoInteractions(publisher);
	}

	private void givenRedisAnswers(Outcome outcome) {
		when(stock.reserve(anyLong(), anyString(), anyInt(), anyInt(), anyString(), anyString(), anyString(), any()))
			.thenAnswer(inv -> new Reservation(outcome, outcome == Outcome.RESERVED ? inv.getArgument(5) : ""));
	}

}
