package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import com.yitaihuang.ticketrush.AbstractIntegrationTest;
import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.order.OrderRequested;
import com.yitaihuang.ticketrush.order.OrderService;
import com.yitaihuang.ticketrush.order.OrderStatus;
import com.yitaihuang.ticketrush.reservation.StockGateway.RebuildStart;
import com.yitaihuang.ticketrush.reservation.StockGateway.SeedOrder;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Rebuilding an event's Redis state after a loss, while the rest of the system
 * keeps running: old senders, the consumer, other instances, and rebuilders
 * that stall or crash halfway.
 */
class RecoveryIntegrationTest extends AbstractIntegrationTest {

	private static final Duration LEASE = Duration.ofSeconds(30);

	@Autowired StockGateway stock;

	@Autowired StockRecovery recovery;

	@Autowired OrderService orderService;

	@Autowired KafkaTemplate<String, String> kafka;

	@Autowired TicketrushProperties properties;

	@Autowired MeterRegistry meters;

	@Autowired RedisRestartGuard guard;

	@Test
	void reservationsWaitOutTheQuietPeriodEvenWhenTheQueueIsEmpty() {
		long eventId = createEvent(10, 4);
		String user = user("quiet");
		reserveAndAwaitOrder(eventId, user, 1);
		wipeRedis(eventId);

		assertThat(recovery.rebuild(eventId)).isFalse(); // lag is 0, but a send from before the loss may still land
		assertThatThrownBy(() -> reservations.reserve(eventId, user, 1, "k2"))
			.hasFieldOrPropertyWithValue("code", "STOCK_RECOVERING");

		quietPeriodOver(eventId);
		assertThat(recovery.rebuild(eventId)).isTrue();
		assertStoresAgree(eventId);
	}

	@Test
	void anOrderAnOldSenderDeliversDuringTheQuietPeriodKeepsItsIdempotencyKey() {
		long eventId = createEvent(10, 4);
		String user = user("old-sender");
		wipeRedis(eventId);
		assertThat(recovery.rebuild(eventId)).isFalse(); // loss noticed; quiet period starts

		// A request accepted just before the loss finally gets its message out.
		String orderId = UUID.randomUUID().toString();
		kafka.send(properties.orders().topic(), String.valueOf(eventId), message(orderId, eventId, user, 1, "k1"))
			.join();
		await().atMost(Duration.ofSeconds(15)).until(() -> orders.existsById(orderId));

		quietPeriodOver(eventId);
		assertThat(recovery.rebuild(eventId)).isTrue();
		assertThat(reservations.reserve(eventId, user, 1, "k1").orderId()).isEqualTo(orderId); // a retry, not a new order
		awaitStoresAgree(eventId);
		assertThat(stock.available(eventId)).hasValue(9);
	}

	@Test
	void anOrderThatReachesKafkaOnlyAfterTheRebuildIsRejectedWhateverItsSendersClockSays() {
		long eventId = createEvent(10, 2);
		String user = user("late-sender");
		// Requests reserved tickets, then paused before their send for longer than the quiet
		// period. Meanwhile Redis lost the event and was rebuilt without those orders.
		wipeRedis(eventId);
		quietPeriodOver(eventId);
		assertThat(recovery.rebuild(eventId)).isTrue();
		String newer = reserveAndAwaitOrder(eventId, user, 2); // the user's whole limit, under the rebuilt state

		String late = UUID.randomUUID().toString();
		send(eventId, message(late, eventId, user, 2, "k-late", Instant.now().minusSeconds(5)));
		// The same, from an instance whose clock runs a minute ahead of Redis's.
		String fastClock = UUID.randomUUID().toString();
		send(eventId, message(fastClock, eventId, user, 1, "k-fast", Instant.now().plusSeconds(60)));
		await().atMost(Duration.ofSeconds(15)).until(() -> orders.existsById(late) && orders.existsById(fastClock));

		assertThat(orders.findById(late).orElseThrow().getStatus()).isEqualTo(OrderStatus.REJECTED);
		assertThat(orders.findById(fastClock).orElseThrow().getStatus()).isEqualTo(OrderStatus.REJECTED);
		assertThat(orders.findById(newer).orElseThrow().getStatus()).isEqualTo(OrderStatus.PENDING_PAYMENT);
		awaitStoresAgree(eventId);
		assertThat(stock.available(eventId)).hasValue(8); // the late orders took nothing; the limit held
	}

	@Test
	void aRedisThatCameBackWithOlderDataStopsSellingUntilItIsRebuilt() {
		long eventId = createEvent(10, 2);
		String user = user("stale-restore");
		String first = reserveAndAwaitOrder(eventId, user, 2);
		// Redis restarts from an AOF/RDB file written before that order: the stock key is
		// there, but the order, its tickets and the user's count are not.
		String prefix = "ticketrush:{event:" + eventId + "}:";
		redis.opsForHash().delete(prefix + "orders", first);
		redis.opsForValue().increment(prefix + "stock", 2);
		redis.opsForValue().decrement(prefix + "held", 2);
		redis.opsForHash().delete(prefix + "buyers", user);
		redis.delete(redis.keys(prefix + "idem:*"));
		redis.opsForValue().set(RedisRestartGuard.SERVER_KEY, "the-server-before-the-restart");

		guard.check();

		assertThatThrownBy(() -> reservations.reserve(eventId, user, 2, "k2"))
			.hasFieldOrPropertyWithValue("code", "STOCK_RECOVERING");
		quietPeriodOver(eventId);
		assertThat(recovery.rebuild(eventId)).isTrue();
		assertThatThrownBy(() -> reservations.reserve(eventId, user, 2, "k2"))
			.hasFieldOrPropertyWithValue("code", "PURCHASE_LIMIT_EXCEEDED");
		assertStoresAgree(eventId);
		assertThat(stock.available(eventId)).hasValue(8);
	}

	@Test
	void aLiveRecountNeverCreatesTheStockKeyOfAnEventBeingRebuilt() {
		long eventId = createEvent(10, 4);
		reserveAndAwaitOrder(eventId, user("half-seeded"), 2);
		wipeRedis(eventId);

		// A reconciler that saw the stock key just before the loss now recounts a half-empty hash.
		stock.recount(eventId, 10);

		assertThat(stock.available(eventId)).isEmpty();
	}

	@Test
	void aRebuilderThatLostItsLeaseCannotWriteIntoTheNextRebuild() {
		long eventId = createEvent(10, 4);
		String alice = user("alice-fence");
		String bob = user("bob-fence");
		String alicesOrder = reserveAndAwaitOrder(eventId, alice, 2);
		String bobsOrder = reserveAndAwaitOrder(eventId, bob, 1);
		wipeRedis(eventId);
		quietPeriodOver(eventId);

		// Instance A starts rebuilding, seeds a snapshot in which bob's order is still active, then stalls.
		String tokenA = "instance-a";
		assertThat(stock.beginRebuild(eventId, tokenA, LEASE, Duration.ZERO)).isEqualTo(RebuildStart.GO);
		List<SeedOrder> staleSnapshot = List.of(seedOf(bobsOrder, bob, true));
		redis.delete("ticketrush:{event:" + eventId + "}:rebuild"); // A's lease runs out while it is stalled

		// Meanwhile bob cancels, and instance B rebuilds from a fresh snapshot.
		orderService.cancel(bobsOrder, bob);
		assertThat(recovery.rebuild(eventId)).isTrue();

		// Redis is lost once more, and A wakes up and carries on with its stale work.
		wipeRedis(eventId);
		assertThat(stock.seed(eventId, tokenA, LEASE, staleSnapshot)).isFalse();
		assertThat(stock.finishRebuild(eventId, tokenA, 10)).isEmpty();
		assertThat(stock.available(eventId)).isEmpty(); // A could not publish its stale count

		quietPeriodOver(eventId);
		assertThat(recovery.rebuild(eventId)).isTrue();
		assertStoresAgree(eventId);
		assertThat(stock.available(eventId)).hasValue(8); // only alice's 2 tickets are held
		assertThat(orders.findById(alicesOrder)).isPresent();
	}

	@Test
	void manyInstancesRecoveringAtOnceRebuildExactlyOnce() throws Exception {
		long eventId = createEvent(10, 4);
		reserveAndAwaitOrder(eventId, user("crowd-a"), 2);
		reserveAndAwaitOrder(eventId, user("crowd-b"), 1);
		wipeRedis(eventId);
		quietPeriodOver(eventId);
		double before = meters.counter("ticketrush.stock.rebuilds").count();

		CountDownLatch start = new CountDownLatch(1);
		List<Future<Boolean>> attempts = new ArrayList<>();
		try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
			for (int i = 0; i < 8; i++) {
				attempts.add(pool.submit(() -> {
					start.await();
					return recovery.rebuild(eventId);
				}));
			}
			start.countDown();
		}

		assertThat(meters.counter("ticketrush.stock.rebuilds").count()).isEqualTo(before + 1);
		assertThat(attempts.stream().filter(f -> f.resultNow()).count()).isPositive();
		assertStoresAgree(eventId);
		assertThat(stock.available(eventId)).hasValue(7);
	}

	@Test
	void aRebuildThatCrashedHalfwayIsRetriedOnceItsLeaseExpires() {
		long eventId = createEvent(10, 4);
		String user = user("crash");
		String orderId = reserveAndAwaitOrder(eventId, user, 2);
		wipeRedis(eventId);
		quietPeriodOver(eventId);

		// Instance A gets the lease, seeds part of the state, and dies.
		assertThat(stock.beginRebuild(eventId, "crashed", LEASE, Duration.ZERO)).isEqualTo(RebuildStart.GO);
		assertThat(stock.seed(eventId, "crashed", LEASE, List.of(seedOf(orderId, user, true)))).isTrue();

		assertThat(recovery.rebuild(eventId)).isFalse(); // its lease is still live
		assertThatThrownBy(() -> reservations.reserve(eventId, user, 1, "k2"))
			.hasFieldOrPropertyWithValue("code", "STOCK_RECOVERING");

		redis.delete("ticketrush:{event:" + eventId + "}:rebuild"); // the lease expires
		assertThat(recovery.rebuild(eventId)).isTrue();
		assertStoresAgree(eventId);
		assertThat(stock.available(eventId)).hasValue(8);
	}

	private String user(String name) {
		return String.valueOf(userId(name));
	}

	private String message(String orderId, long eventId, String user, int quantity, String key) {
		return message(orderId, eventId, user, quantity, key, Instant.now());
	}

	private String message(String orderId, long eventId, String user, int quantity, String key, Instant at) {
		return json.writeValueAsString(new OrderRequested(orderId, eventId, user, quantity, quantity * 2500, key, at));
	}

	private void send(long eventId, String message) {
		kafka.send(properties.orders().topic(), String.valueOf(eventId), message).join();
	}

	/** What a rebuilder would seed for this order, as of a snapshot where it is (or isn't) active. */
	private SeedOrder seedOf(String orderId, String user, boolean active) {
		var order = orders.findById(orderId).orElseThrow();
		String message = active ? json.writeValueAsString(OrderRequested.of(order)) : null;
		return new SeedOrder(orderId, user, order.getIdempotencyKey(), message, Duration.ofHours(1));
	}

}
