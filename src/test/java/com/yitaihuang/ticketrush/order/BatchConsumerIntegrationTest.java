package com.yitaihuang.ticketrush.order;

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
import org.springframework.kafka.listener.BatchListenerFailedException;

import com.yitaihuang.ticketrush.AbstractIntegrationTest;
import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.reservation.StockGateway;

/**
 * The consumer writes a poll's messages in one transaction. Batching must not
 * change what happens to any single order: duplicates, redeliveries after the
 * commit, a bad neighbour, a rolled-back batch, and two consumers on the same
 * messages during a rebalance.
 */
class BatchConsumerIntegrationTest extends AbstractIntegrationTest {

	@Autowired OrderConsumer consumer;

	@Autowired StockGateway stock;

	@Autowired OrderFailureService failures;

	@Autowired KafkaTemplate<String, String> kafka;

	@Autowired TicketrushProperties properties;

	@Test
	void theSameOrderTwiceInOneBatchIsStoredOnce() {
		long eventId = createEvent(10, 4);
		String a = reserveInRedis(eventId, "batch-dup-a", 2);
		String b = reserveInRedis(eventId, "batch-dup-b", 1);

		consumer.onOrderRequests(List.of(a, a, b));

		assertThat(orders.findByEventId(eventId)).hasSize(2);
		assertStoresAgree(eventId);
		assertThat(eventRepository.findById(eventId).orElseThrow().getAvailableTickets()).isEqualTo(7);
	}

	@Test
	void aBatchRedeliveredAfterItsCommitChangesNothing() {
		long eventId = createEvent(10, 4);
		List<String> batch = List.of(reserveInRedis(eventId, "redo-a", 1), reserveInRedis(eventId, "redo-b", 2));

		consumer.onOrderRequests(batch);
		// The app died after the commit, before Kafka got the offsets: the whole batch comes again.
		consumer.onOrderRequests(batch);

		assertThat(orders.findByEventId(eventId)).hasSize(2);
		assertStoresAgree(eventId);
		assertThat(eventRepository.findById(eventId).orElseThrow().getAvailableTickets()).isEqualTo(7);
	}

	@Test
	void aBadMessageFailsAloneAndTheOffsetStopsRightAtIt() {
		long eventId = createEvent(10, 4);
		String before = reserveInRedis(eventId, "bad-before", 1);
		String after = reserveInRedis(eventId, "bad-after", 1);

		assertThatThrownBy(() -> consumer.onOrderRequests(List.of(before, "not json", after)))
			.isInstanceOfSatisfying(BatchListenerFailedException.class, ex -> assertThat(ex.getIndex()).isEqualTo(1));

		// The error handler commits the offset of `before` and redelivers from the bad message on.
		assertThat(orders.existsById(orderId(before))).isTrue();
		assertThat(orders.existsById(orderId(after))).isFalse();
		consumer.onOrderRequests(List.of(after)); // past the bad one, once it is dead-lettered
		assertStoresAgree(eventId);
	}

	@Test
	void aDatabaseFailureRollsTheWholeBatchBackThenOnlyTheGuiltyMessageFails() {
		long eventId = createEvent(10, 4);
		String good = reserveInRedis(eventId, "rollback-good", 1);
		// Longer than orders.user_id: MySQL refuses it, at the batch's flush.
		String bad = reserveInRedis(eventId, "u".repeat(80), 2);
		String later = reserveInRedis(eventId, "rollback-later", 3);

		assertThatThrownBy(() -> consumer.onOrderRequests(List.of(good, bad, later)))
			.isInstanceOfSatisfying(BatchListenerFailedException.class, ex -> assertThat(ex.getIndex()).isEqualTo(1));

		// The batch's decrements were rolled back; only the order redone on its own before the bad one stands.
		assertThat(orders.findByEventId(eventId)).extracting(Order::getId).containsExactly(orderId(good));
		assertThat(eventRepository.findById(eventId).orElseThrow().getAvailableTickets()).isEqualTo(9);

		consumer.onOrderRequests(List.of(later)); // redelivered after the bad one went to the DLT
		assertThat(eventRepository.findById(eventId).orElseThrow().getAvailableTickets()).isEqualTo(6);
		failures.record(bad, "DataIntegrityViolationException", "test-batch-bad@1");
		failures.voidOrder(failures.list(null).stream()
			.filter(f -> bad.equals(f.getPayload()))
			.findFirst()
			.orElseThrow()
			.getId(), null);
		assertStoresAgree(eventId);
	}

	@Test
	void throughKafkaTheGoodNeighboursOfAPoisonMessageAreAllStored() {
		long eventId = createEvent(10, 4);
		String first = reserveInRedis(eventId, "poison-first", 1);
		String last = reserveInRedis(eventId, "poison-last", 1);
		String poison = "poison " + UUID.randomUUID();
		String key = String.valueOf(eventId); // one partition, in this order, likely one poll
		kafka.send(properties.orders().topic(), key, first);
		kafka.send(properties.orders().topic(), key, poison);
		kafka.send(properties.orders().topic(), key, last).join();

		await().atMost(Duration.ofSeconds(30))
			.until(() -> orders.existsById(orderId(first)) && orders.existsById(orderId(last))
					&& failures.list(OrderFailure.Status.OPEN).stream().anyMatch(f -> poison.equals(f.getPayload())));
		awaitStoresAgree(eventId);
	}

	@Test
	void twoConsumersOnOverlappingBatchesDuringARebalanceStoreEachOrderOnce() throws Exception {
		long eventId = createEvent(20, 4);
		List<String> messages = new ArrayList<>();
		for (int i = 0; i < 6; i++) {
			messages.add(reserveInRedis(eventId, "rebalance-" + i, 1));
		}
		// The old owner of the partition still works on its batch while the new owner gets an overlapping one.
		List<List<String>> batches = List.of(messages.subList(0, 4), messages.subList(2, 6));

		CountDownLatch start = new CountDownLatch(1);
		try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
			List<Future<?>> running = new ArrayList<>();
			for (List<String> batch : batches) {
				running.add(pool.submit(() -> {
					start.await();
					redeliverUntilDone(batch);
					return null;
				}));
			}
			start.countDown();
			for (Future<?> future : running) {
				future.get();
			}
		}

		assertThat(orders.findByEventId(eventId)).hasSize(6);
		assertStoresAgree(eventId);
		assertThat(eventRepository.findById(eventId).orElseThrow().getAvailableTickets()).isEqualTo(14);
	}

	/** What the error handler does with a failed batch: redeliver from the failed message on. */
	private void redeliverUntilDone(List<String> batch) {
		List<String> left = batch;
		for (int attempt = 0; attempt < 5 && !left.isEmpty(); attempt++) {
			try {
				consumer.onOrderRequests(left);
				left = List.of();
			}
			catch (BatchListenerFailedException ex) {
				left = left.subList(ex.getIndex(), left.size()); // e.g. a deadlock between the two
			}
		}
		assertThat(left).isEmpty();
	}

	private String reserveInRedis(long eventId, String user, int quantity) {
		String userId = user.length() > 64 ? user : String.valueOf(userId(user));
		String orderId = UUID.randomUUID().toString();
		String message = json.writeValueAsString(
				new OrderRequested(orderId, eventId, userId, quantity, quantity * 2500, "k1", Instant.now()));
		stock.reserve(eventId, userId, quantity, 4, "k1", orderId, message);
		return message;
	}

	private String orderId(String message) {
		return json.readValue(message, OrderRequested.class).orderId();
	}

}
