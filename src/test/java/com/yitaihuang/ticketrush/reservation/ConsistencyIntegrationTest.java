package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import com.yitaihuang.ticketrush.AbstractIntegrationTest;
import com.yitaihuang.ticketrush.common.ApiException;
import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.event.EventCatalog;
import com.yitaihuang.ticketrush.order.OrderConsumer;
import com.yitaihuang.ticketrush.order.OrderFailureService;
import com.yitaihuang.ticketrush.order.OrderRequested;
import com.yitaihuang.ticketrush.order.OrderService;
import com.yitaihuang.ticketrush.order.OrderStatus;
import com.yitaihuang.ticketrush.order.PendingOrderTracker;
import com.yitaihuang.ticketrush.reservation.OrderRequestPublisher.Delivery;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Failure windows between Redis, Kafka and MySQL. Each test drives one window
 * through the real pipeline and then checks every invariant at once
 * ({@link #assertStoresAgree}), not just that stock stayed non-negative.
 */
class ConsistencyIntegrationTest extends AbstractIntegrationTest {

	@Autowired StockGateway stock;

	@Autowired StockRecovery recovery;

	@Autowired StockReconciler reconciler;

	@Autowired OrderOutboxRelay relay;

	@Autowired OrderService orderService;

	@Autowired OrderConsumer consumer;

	@Autowired OrderFailureService failures;

	@Autowired OrderQueueLag lag;

	@Autowired KafkaTemplate<String, String> kafka;

	@Autowired TicketrushProperties properties;

	@Autowired EventCatalog catalog;

	@Autowired RateLimiter rateLimiter;

	@Autowired PendingOrderTracker pendingOrders;

	@Autowired MeterRegistry meters;

	@Autowired JdbcTemplate jdbc;

	// --- releases ---------------------------------------------------------

	@Test
	void releasingAnOrderTwiceReturnsItsTicketsOnce() {
		long eventId = createEvent(5, 4);
		String user = user("rel");
		String orderId = reserveAndAwaitOrder(eventId, user, 2);

		orderService.cancel(orderId, user);
		boolean again = stock.release(eventId, orderId); // a retry, or the reconciler racing the cancel

		assertThat(again).isFalse();
		assertThat(stock.available(eventId)).hasValue(5);
		assertStoresAgree(eventId);
	}

	@Test
	void aReleaseLostAfterTheCommitIsAppliedByTheReconciler() {
		long eventId = createEvent(5, 4);
		String user = user("lost-release");
		String orderId = reserveAndAwaitOrder(eventId, user, 2);
		// The app died right after MySQL committed the cancellation, before Redis heard of it.
		jdbc.update("UPDATE orders SET status = 'CANCELLED' WHERE id = ?", orderId);
		jdbc.update("UPDATE events SET available_tickets = available_tickets + 2 WHERE id = ?", eventId);
		double before = meters.counter("ticketrush.stock.late_releases").count();

		reconciler.run();

		assertThat(stock.available(eventId)).hasValue(5);
		assertThat(meters.counter("ticketrush.stock.late_releases").count()).isEqualTo(before + 1);
		assertStoresAgree(eventId);
	}

	// --- reservation hand-off ----------------------------------------------

	@Test
	void aRetryAfterTheRedisReplyWasLostGetsTheSameOrderPersistedOnce() {
		long eventId = createEvent(10, 4);
		String user = user("lost-reply");
		String orderId = UUID.randomUUID().toString();
		// reserve.lua ran, but the reply never reached the app, so nothing was published.
		stock.reserve(eventId, user, 1, 4, "k1", orderId, orderJson(orderId, eventId, user, 1, "k1"), SALE_OPEN);

		ReservationService.Result retry = reservations.reserve(eventId, user, 1, "k1");
		relay.relay(eventId);

		assertThat(retry).isEqualTo(new ReservationService.Result(orderId, true));
		awaitStoresAgree(eventId);
		assertThat(orders.findByEventId(eventId)).singleElement()
			.satisfies(order -> assertThat(order.getId()).isEqualTo(orderId));
	}

	@Test
	void anAckTimeoutAfterKafkaStoredTheOrderKeepsItsTickets() {
		long eventId = createEvent(10, 4);
		String user = user("ack-lost");
		ReservationService service = serviceWith(new OrderRequestPublisher(kafka, properties) {
			@Override
			public Delivery publish(long eventId, String orderId, String orderJson) {
				super.publish(eventId, orderId, orderJson); // stored by the broker...
				return Delivery.UNKNOWN; // ...but the ack never came back
			}
		});

		String orderId = service.reserve(eventId, user, 2, "k1").orderId();

		awaitStoresAgree(eventId);
		assertThat(relay.relay(eventId)).isZero(); // the consumer's confirmation emptied the outbox
		assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.PENDING_PAYMENT);
		assertThat(stock.available(eventId)).hasValue(8);
	}

	@Test
	void anAckTimeoutWhenKafkaNeverGotTheOrderIsResentByTheRelay() {
		long eventId = createEvent(10, 4);
		String user = user("never-sent");
		ReservationService service = serviceWith(new OrderRequestPublisher(kafka, properties) {
			@Override
			public Delivery publish(long eventId, String orderId, String orderJson) {
				return Delivery.UNKNOWN; // timed out before the broker stored it
			}
		});

		String orderId = service.reserve(eventId, user, 2, "k1").orderId();
		assertThat(stock.outboxSize(eventId)).isOne();
		assertThat(stock.available(eventId)).hasValue(8); // not released: it might be in Kafka

		assertThat(relay.relay(eventId)).isOne();

		awaitStoresAgree(eventId);
		assertThat(orders.findById(orderId)).isPresent();
		assertThat(reservations.reserve(eventId, user, 2, "k1").orderId()).isEqualTo(orderId);
	}

	@Test
	void anOrderThatDefinitelyWasNotSentIsReleasedAndItsKeyForgotten() {
		long eventId = createEvent(10, 4);
		String user = user("not-sent");
		ReservationService service = serviceWith(new OrderRequestPublisher(kafka, properties) {
			@Override
			public Delivery publish(long eventId, String orderId, String orderJson) {
				return Delivery.NOT_SENT;
			}
		});

		assertThatThrownBy(() -> service.reserve(eventId, user, 2, "k1")).isInstanceOf(ApiException.class)
			.hasFieldOrPropertyWithValue("status", HttpStatus.SERVICE_UNAVAILABLE);
		assertThat(stock.available(eventId)).hasValue(10);

		ReservationService.Result retry = reservations.reserve(eventId, user, 2, "k1");
		assertThat(retry.replayed()).isFalse(); // a fresh attempt, not the failed one
		await().atMost(Duration.ofSeconds(15)).until(() -> orders.existsById(retry.orderId()));
		awaitStoresAgree(eventId);
	}

	@Test
	void anUnconfirmedOrderOlderThanTheLimitPausesNewReservations() {
		long eventId = createEvent(10, 4);
		String user = user("backlog");
		// An order Kafka has not confirmed for a long time (score 0 = 1970).
		redis.opsForZSet().add("ticketrush:{event:" + eventId + "}:outbox", "stuck-order", 0);

		assertThatThrownBy(() -> reservations.reserve(eventId, user, 1, "k1"))
			.hasFieldOrPropertyWithValue("code", "ORDER_QUEUE_UNAVAILABLE");

		relay.relay(eventId); // the entry has no order behind it, so the relay drops it
		reserveAndAwaitOrder(eventId, user, 1);
		awaitStoresAgree(eventId);
	}

	// --- consumer -----------------------------------------------------------

	@Test
	void aRedeliveredMessageIsAppliedOnce() {
		long eventId = createEvent(10, 4);
		String user = user("redeliver");
		String orderId = UUID.randomUUID().toString();
		String message = orderJson(orderId, eventId, user, 2, "k1");
		stock.reserve(eventId, user, 2, 4, "k1", orderId, message, SALE_OPEN);

		consumer.onOrderRequested(message);
		consumer.onOrderRequested(message);

		assertThat(orders.findByEventId(eventId)).hasSize(1);
		assertStoresAgree(eventId);
	}

	@Test
	void aConstraintErrorThatIsNotAReplayIsNotSwallowed() {
		long eventId = createEvent(10, 4);
		String user = user("conflict");
		String first = reserveAndAwaitOrder(eventId, user, 1);
		String original = orders.findById(first).orElseThrow().getIdempotencyKey();

		// Same user and key as a stored order, but a different order id: not a redelivery.
		String clash = orderJson(UUID.randomUUID().toString(), eventId, user, 1, original);
		// An order for an event that doesn't exist: a foreign key violation.
		String orphan = orderJson(UUID.randomUUID().toString(), 987_654_321L, user, 1, "k1");

		assertThatThrownBy(() -> consumer.onOrderRequested(clash))
			.isInstanceOf(DataIntegrityViolationException.class);
		assertThatThrownBy(() -> consumer.onOrderRequested(orphan))
			.isInstanceOf(DataIntegrityViolationException.class);
		assertThat(orders.findByEventId(eventId)).hasSize(1);

		// Redis never reserved the clash, so its claim refused it: it took no tickets. It is
		// dead-lettered all the same (its rejected row clashes too); voiding it changes no stock.
		assertThat(stock.available(eventId)).hasValue(9);
		failures.record(clash, "DataIntegrityViolationException", "test-clash@1");
		failures.voidOrder(failures.list(null).stream()
			.filter(f -> clash.equals(f.getPayload()))
			.findFirst()
			.orElseThrow()
			.getId(), null);
		assertStoresAgree(eventId);
	}

	@Test
	void theSameOrderIdWithDifferentDetailsFails() {
		long eventId = createEvent(10, 4);
		String user = user("tampered");
		String orderId = reserveAndAwaitOrder(eventId, user, 1);
		String key = orders.findById(orderId).orElseThrow().getIdempotencyKey();

		assertThatThrownBy(() -> consumer.onOrderRequested(orderJson(orderId, eventId, user, 3, key)))
			.isInstanceOf(IllegalStateException.class);
		assertStoresAgree(eventId);
	}

	// --- reconciliation -------------------------------------------------------

	@Test
	void reconcilingWhileTheConsumerIsStoppedLeavesOrdersInFlightAlone() {
		long eventId = createEvent(10, 4);
		String user = user("in-flight");
		withConsumerStopped(() -> {
			reservations.reserve(eventId, user, 2, "k1"); // in Kafka, not yet in MySQL
			String unsent = UUID.randomUUID().toString(); // reserved, not even in Kafka yet
			stock.reserve(eventId, user, 1, 4, "k2", unsent, orderJson(unsent, eventId, user, 1, "k2"), SALE_OPEN);
			await().atMost(Duration.ofSeconds(15)).until(() -> lag.current().orElse(0) > 0);

			reconciler.run();
			reconciler.run();

			assertThat(stock.available(eventId)).hasValue(7);
			assertThat(eventRepository.findById(eventId).orElseThrow().getAvailableTickets()).isEqualTo(10);
		});
		relay.relay(eventId);
		awaitStoresAgree(eventId);
		assertThat(orders.findByEventId(eventId)).hasSize(2);
	}

	@Test
	void aHandEditedStockKeyIsRecountedFromTheHeldOrders() {
		long eventId = createEvent(10, 4);
		reserveAndAwaitOrder(eventId, user("drift"), 2);
		stock.initialize(eventId, 5); // e.g. a bad manual fix

		reconciler.run();

		assertThat(stock.available(eventId)).hasValue(8);
		assertStoresAgree(eventId);
	}

	// --- Redis data loss --------------------------------------------------------

	@Test
	void afterRedisLosesAnEventItIsRebuiltWithLimitsIdempotencyAndOrdersInFlight() {
		long eventId = createEvent(10, 2);
		String alice = user("alice-rb");
		String bob = user("bob-rb");
		String carol = user("carol-rb");
		String dave = user("dave-rb");
		String alicesOrder = reservations.reserve(eventId, alice, 2, "alice-1").orderId();
		String bobsOrder = reservations.reserve(eventId, bob, 1, "bob-1").orderId();
		await().atMost(Duration.ofSeconds(15))
			.until(() -> orders.existsById(alicesOrder) && orders.existsById(bobsOrder));
		orderService.cancel(bobsOrder, bob);

		String[] carolsOrder = new String[1];
		withConsumerStopped(() -> {
			carolsOrder[0] = reservations.reserve(eventId, carol, 1, "carol-1").orderId(); // still in Kafka
			await().atMost(Duration.ofSeconds(15)).until(() -> lag.current().orElse(0) > 0);

			wipeRedis(eventId); // Redis restarted empty
			quietPeriodOver(eventId); // sends from before the loss are done; only the queue is left

			// Carol's order is in neither store yet: rebuilding now would forget it, so intake waits.
			assertThatThrownBy(() -> reservations.reserve(eventId, dave, 1, "dave-1"))
				.hasFieldOrPropertyWithValue("code", "STOCK_RECOVERING");
		});

		// Once the queue drains, the next reservation rebuilds Redis and goes through.
		await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(400)).ignoreExceptions()
			.until(() -> reservations.reserve(eventId, dave, 1, "dave-1") != null);

		assertThat(reservations.reserve(eventId, alice, 2, "alice-1").orderId()).isEqualTo(alicesOrder);
		assertThat(reservations.reserve(eventId, bob, 1, "bob-1").orderId()).isEqualTo(bobsOrder);
		assertThat(reservations.reserve(eventId, carol, 1, "carol-1").orderId()).isEqualTo(carolsOrder[0]);
		assertThatThrownBy(() -> reservations.reserve(eventId, alice, 1, "alice-2"))
			.hasFieldOrPropertyWithValue("code", "PURCHASE_LIMIT_EXCEEDED");
		awaitStoresAgree(eventId);
		assertThat(stock.available(eventId)).hasValue(10 - 2 - 1 - 1); // alice, carol, dave; bob cancelled
	}

	// --- helpers ------------------------------------------------------------------

	private String user(String name) {
		return String.valueOf(userId(name));
	}

	private String orderJson(String orderId, long eventId, String userId, int quantity, String idempotencyKey) {
		return json.writeValueAsString(
				new OrderRequested(orderId, eventId, userId, quantity, quantity * 2500, idempotencyKey, Instant.now()));
	}

	private ReservationService serviceWith(OrderRequestPublisher publisher) {
		return new ReservationService(catalog, stock, recovery, rateLimiter, publisher, pendingOrders, json, meters,
				Clock.systemUTC());
	}

}
