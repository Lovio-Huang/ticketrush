package com.yitaihuang.ticketrush.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

import com.yitaihuang.ticketrush.AbstractIntegrationTest;
import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.order.OrderFailure.Status;
import com.yitaihuang.ticketrush.reservation.StockGateway;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Orders the consumer gave up on: they must be visible, keep their tickets held
 * until someone decides, and every decision must be idempotent and final.
 */
class DeadLetterIntegrationTest extends AbstractIntegrationTest {

	@Autowired OrderFailureService failures;

	@Autowired OrderConsumer consumer;

	@Autowired StockGateway stock;

	@Autowired KafkaTemplate<String, String> kafka;

	@Autowired TicketrushProperties properties;

	@Autowired MeterRegistry meters;

	@Autowired MockMvcTester mvc;

	@Autowired OrderService orderService;

	@Autowired TransactionTemplate transactions;

	@Autowired JdbcTemplate jdbc;

	@Test
	void aDeadLetteredOrderIsRecordedCountedAndKeepsItsTicketsHeld() {
		long eventId = createEvent(10, 4);
		// Longer than orders.user_id: MySQL refuses this order on every attempt.
		String user = "u".repeat(80);
		String orderId = UUID.randomUUID().toString();
		String message = reserveInRedis(eventId, orderId, user, 2);
		double before = meters.counter("ticketrush.orders.dead_lettered").count();

		send(eventId, message);
		OrderFailure failure = awaitFailure(orderId, f -> true);

		assertThat(failure.getStatus()).isEqualTo(Status.OPEN);
		assertThat(failure.getPayload()).isEqualTo(message);
		assertThat(failure.getError()).isNotBlank();
		assertThat(meters.counter("ticketrush.orders.dead_lettered").count()).isGreaterThan(before);
		assertThat(meters.get("ticketrush.orders.failures.open").gauge().value()).isPositive();
		assertThat(stock.available(eventId)).hasValue(8); // held, not guessed back
		assertThat(orders.existsById(orderId)).isFalse();
	}

	@Test
	void aVoidedOrderReleasesItsTicketsOnceAndALateCopyCannotReviveIt() {
		long eventId = createEvent(10, 4);
		String user = "v".repeat(80); // no rejected row can be stored for it either
		String orderId = UUID.randomUUID().toString();
		String message = reserveInRedis(eventId, orderId, user, 2);
		send(eventId, message);
		long id = awaitFailure(orderId, f -> true).getId();

		failures.voidOrder(id, "user id too long");
		failures.voidOrder(id, "clicked twice"); // idempotent

		assertThat(stock.available(eventId)).hasValue(10);
		send(eventId, message); // a stray copy turns up after the decision
		OrderFailure failure = awaitFailure(orderId, f -> f.getAttempts() == 2);
		assertThat(failure.getStatus()).isEqualTo(Status.VOIDED);
		assertThat(failure.getNote()).isEqualTo("user id too long");
		assertThat(orders.existsById(orderId)).isFalse();
		awaitStoresAgree(eventId);
		assertThat(stock.available(eventId)).hasValue(10);
	}

	@Test
	void aVoidedOrderIsStoredAsRejectedSoRedeliveriesAreNoOps() {
		long eventId = createEvent(10, 4);
		String user = String.valueOf(userId("void-row"));
		String orderId = UUID.randomUUID().toString();
		String message = reserveInRedis(eventId, orderId, user, 2);
		// As if the consumer had dead-lettered it during, say, a long MySQL outage.
		failures.record(message, "CannotCreateTransactionException: MySQL down", "test-void-row@1");
		long id = failureOf(orderId).getId();

		failures.voidOrder(id, null);
		consumer.onOrderRequested(message); // the original finally arrives

		assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.REJECTED);
		assertStoresAgree(eventId);
		assertThat(stock.available(eventId)).hasValue(10);
		assertThat(eventRepository.findById(eventId).orElseThrow().getAvailableTickets()).isEqualTo(10);
	}

	@Test
	void replayingSendsTheSameOrderAgainAndIsIdempotent() {
		long eventId = createEvent(10, 4);
		String user = String.valueOf(userId("replay"));
		String orderId = UUID.randomUUID().toString();
		String message = reserveInRedis(eventId, orderId, user, 2);
		failures.record(message, "CannotCreateTransactionException: MySQL down", "test-replay@1");
		long id = failureOf(orderId).getId();

		assertThat(failures.replay(id, "MySQL is back").getStatus()).isEqualTo(Status.REPLAYED);
		failures.replay(id, "again");

		await().atMost(Duration.ofSeconds(15)).until(() -> orders.existsById(orderId));
		awaitStoresAgree(eventId);
		assertThat(orders.findByEventId(eventId)).hasSize(1);
		assertThatThrownBy(() -> failures.voidOrder(id, null)).hasFieldOrPropertyWithValue("code", "ORDER_ACTIVE");
	}

	@Test
	void theSameDeadLetterTwiceIsRecordedOnceAndARepeatFailureCountsAsAnAttempt() {
		long eventId = createEvent(10, 4);
		String orderId = UUID.randomUUID().toString();
		String message = reserveInRedis(eventId, orderId, String.valueOf(userId("dup")), 1);

		failures.record(message, "first", "test-dup@1");
		failures.record(message, "first", "test-dup@1"); // the DLT listener got the record again
		assertThat(failureOf(orderId).getAttempts()).isOne();

		failures.record(message, "second", "test-dup@2"); // a replay that failed again
		failures.record(message, "second", "test-dup@2"); // and that record redelivered (rebalance)
		OrderFailure failure = failureOf(orderId);
		assertThat(failure.getAttempts()).isEqualTo(2);
		assertThat(failure.getError()).isEqualTo("second");
	}

	@Test
	void aVoidRacingTheConsumerOfAReplayedCopyNeverReleasesAnActiveOrder() throws Exception {
		long eventId = createEvent(10, 4);
		String orderId = UUID.randomUUID().toString();
		String message = reserveInRedis(eventId, orderId, String.valueOf(userId("void-race")), 2);
		failures.record(message, "CannotCreateTransactionException: MySQL down", "test-void-race@1");
		long id = failureOf(orderId).getId();
		OrderRequested request = json.readValue(message, OrderRequested.class);

		// The consumer of a replayed copy has inserted the order but not committed yet.
		CountDownLatch inserted = new CountDownLatch(1);
		CountDownLatch commit = new CountDownLatch(1);
		try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
			Future<?> consuming = pool.submit(() -> transactions.executeWithoutResult(tx -> {
				orderService.recordRequest(request);
				orders.flush();
				inserted.countDown();
				awaitLatch(commit);
			}));
			awaitLatch(inserted);
			// The void sees no order yet, and its rejected row waits on the consumer's row lock.
			Future<OrderFailure> voiding = pool.submit(() -> failures.voidOrder(id, "race"));
			await().atMost(Duration.ofSeconds(10)).until(() -> lockWaits() > 0);
			commit.countDown();
			consuming.get();

			assertThatThrownBy(voiding::get).cause().hasFieldOrPropertyWithValue("code", "ORDER_ACTIVE");
		}
		assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.PENDING_PAYMENT);
		assertThat(failureOf(orderId).getStatus()).isEqualTo(Status.OPEN);
		assertStoresAgree(eventId);
		assertThat(stock.available(eventId)).hasValue(8);
	}

	@Test
	void aDeadLetterThatCannotBeOneOfOurOrdersIsStillRecorded() {
		// Parses as an order, but its id is longer than any order id: it can't hold tickets,
		// and storing it as an order id would fail forever and block the DLT listener.
		String message = json.writeValueAsString(
				new OrderRequested("x".repeat(60), 1L, "1", 1, 2500, "k1", Instant.now()));

		failures.record(message, "boom", "test-odd-id@1");

		OrderFailure failure = failures.list(Status.OPEN)
			.stream()
			.filter(f -> message.equals(f.getPayload()))
			.findFirst()
			.orElseThrow();
		assertThat(failure.getOrderId()).isNull();
		assertThat(failures.discard(failure.getId(), null).getStatus()).isEqualTo(Status.DISCARDED);
	}

	private void awaitLatch(CountDownLatch latch) {
		try {
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(ex);
		}
	}

	/** Inserts into orders still running: here, only one blocked on another transaction's row lock. */
	private int lockWaits() {
		return jdbc.queryForObject("SELECT COUNT(*) FROM information_schema.processlist "
				+ "WHERE command = 'Query' AND info LIKE 'insert into orders %'", Integer.class);
	}

	@Test
	void anUnreadableMessageKeepsItsDiagnosticsAndCanOnlyBeDiscarded() {
		String garbage = "not an order " + UUID.randomUUID();
		kafka.send(properties.orders().topic(), "1", garbage).join();

		await().atMost(Duration.ofSeconds(20))
			.until(() -> failures.list(Status.OPEN).stream().anyMatch(f -> garbage.equals(f.getPayload())));
		OrderFailure failure = failures.list(Status.OPEN)
			.stream()
			.filter(f -> garbage.equals(f.getPayload()))
			.findFirst()
			.orElseThrow();

		assertThat(failure.getOrderId()).isNull();
		assertThat(failure.getError()).contains("jackson").contains("Unrecognized token");
		assertThatThrownBy(() -> failures.voidOrder(failure.getId(), null))
			.hasFieldOrPropertyWithValue("code", "NOT_AN_ORDER");
		assertThatThrownBy(() -> failures.replay(failure.getId(), null))
			.hasFieldOrPropertyWithValue("code", "NOT_AN_ORDER");
		assertThat(failures.discard(failure.getId(), "garbage").getStatus()).isEqualTo(Status.DISCARDED);
		assertThat(failures.discard(failure.getId(), "again").getStatus()).isEqualTo(Status.DISCARDED);
	}

	@Test
	void aJsonNullDeadLetterIsKeptAsUnreadableAndDoesNotBlockTheOnesAfterIt() {
		long eventId = createEvent(10, 4);
		String orderId = UUID.randomUUID().toString();
		String next = reserveInRedis(eventId, orderId, String.valueOf(userId("after-null")), 1);
		// Same key, same DLT partition: the listener must get past "null" to reach the next one.
		String dlt = properties.orders().topic() + ".DLT";
		kafka.send(dlt, "null-key", "null").join();
		kafka.send(dlt, "null-key", next).join();

		awaitFailure(orderId, f -> true);
		OrderFailure unreadable = failures.list(Status.OPEN)
			.stream()
			.filter(f -> "null".equals(f.getPayload()))
			.findFirst()
			.orElseThrow();
		assertThat(unreadable.getOrderId()).isNull();
		assertThatThrownBy(() -> failures.voidOrder(unreadable.getId(), null))
			.hasFieldOrPropertyWithValue("code", "NOT_AN_ORDER");
		assertThat(failures.discard(unreadable.getId(), "json null").getStatus()).isEqualTo(Status.DISCARDED);
	}

	@Test
	void emptyAndShapelessDeadLettersAreRecordedAsUnreadable() {
		String tag = UUID.randomUUID().toString();
		String[] payloads = { "", "{}", "[]", "\"" + tag + "\"", "{\"eventId\":1,\"note\":\"" + tag + "\"}" };
		for (int i = 0; i < payloads.length; i++) {
			failures.record(payloads[i], "boom", "test-shapeless-" + tag + "@" + i);
		}

		var recorded = failures.list(Status.OPEN)
			.stream()
			.filter(f -> f.getDltPosition().startsWith("test-shapeless-" + tag))
			.toList();
		assertThat(recorded).hasSize(5).allSatisfy(f -> assertThat(f.getOrderId()).isNull());
		recorded.forEach(f -> assertThat(failures.discard(f.getId(), null).getStatus()).isEqualTo(Status.DISCARDED));
	}

	@Test
	void anOrderThatStillHoldsTicketsCannotBeDiscarded() {
		long eventId = createEvent(10, 4);
		String orderId = UUID.randomUUID().toString();
		String message = reserveInRedis(eventId, orderId, String.valueOf(userId("no-discard")), 1);
		failures.record(message, "boom", "test-no-discard@1");
		long id = failureOf(orderId).getId();

		assertThatThrownBy(() -> failures.discard(id, null))
			.hasFieldOrPropertyWithValue("code", "ORDER_HOLDS_TICKETS");
		assertThat(stock.available(eventId)).hasValue(9);
	}

	@Test
	void onlyAdminsSeeOrResolveFailures() {
		assertThat(mvc.get().uri("/api/admin/order-failures").header("Authorization", bearer("mallory")))
			.hasStatus(HttpStatus.FORBIDDEN);
		assertThat(mvc.post().uri("/api/admin/order-failures/1/void").header("Authorization", bearer("mallory")))
			.hasStatus(HttpStatus.FORBIDDEN);
		assertThat(mvc.get().uri("/api/admin/order-failures?status=OPEN").header("Authorization", adminBearer()))
			.hasStatusOk();
	}

	private String reserveInRedis(long eventId, String orderId, String user, int quantity) {
		String message = json.writeValueAsString(
				new OrderRequested(orderId, eventId, user, quantity, quantity * 2500, "k1", Instant.now()));
		stock.reserve(eventId, user, quantity, 4, "k1", orderId, message, SALE_OPEN);
		return message;
	}

	private OrderFailure failureOf(String orderId) {
		return failures.list(null).stream().filter(f -> orderId.equals(f.getOrderId())).findFirst().orElseThrow();
	}

	private void send(long eventId, String message) {
		kafka.send(properties.orders().topic(), String.valueOf(eventId), message).join();
	}

	private OrderFailure awaitFailure(String orderId, java.util.function.Predicate<OrderFailure> condition) {
		Optional<OrderFailure>[] found = new Optional[1];
		await().atMost(Duration.ofSeconds(20)).until(() -> {
			found[0] = failures.list(null).stream().filter(f -> orderId.equals(f.getOrderId())).findFirst();
			return found[0].filter(condition).isPresent();
		});
		return found[0].orElseThrow();
	}

}
