package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;

import com.yitaihuang.ticketrush.AbstractIntegrationTest;
import com.yitaihuang.ticketrush.common.ApiException;
import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.event.EventCatalog;
import com.yitaihuang.ticketrush.order.OrderConsumer;
import com.yitaihuang.ticketrush.order.OrderRequested;
import com.yitaihuang.ticketrush.order.OrderStatus;
import com.yitaihuang.ticketrush.order.PendingOrderTracker;
import com.yitaihuang.ticketrush.reservation.OrderRequestPublisher.Delivery;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * The request that decides "never sent" and releases, versus the relay or the
 * consumer that may already be sending or storing the same order. Latches pin
 * each interleaving down; whichever side wins, the order must end up either
 * active and held, or released and not active.
 */
class PublishReleaseRaceIntegrationTest extends AbstractIntegrationTest {

	@Autowired StockGateway stock;

	@Autowired StockRecovery recovery;

	@Autowired OrderOutboxRelay relay;

	@Autowired OrderConsumer consumer;

	@Autowired KafkaTemplate<String, String> kafka;

	@Autowired TicketrushProperties properties;

	@Autowired MeterRegistry meters;

	@Autowired EventCatalog catalog;

	@Autowired RateLimiter rateLimiter;

	@Autowired PendingOrderTracker pendingOrders;

	@Test
	void aRelayThatAlreadyPickedTheOrderWinsOverALateNotSentRelease() throws Exception {
		long eventId = createEvent(10, 4);
		String user = user("relay-first");
		String orderId = reserveInRedis(eventId, user, 2, "k1");
		CountDownLatch picked = new CountDownLatch(1);
		CountDownLatch proceed = new CountDownLatch(1);
		OrderOutboxRelay pausedRelay = relayWith(new OrderRequestPublisher(kafka, properties) {
			@Override
			public Delivery publish(long eventId, String orderId, String orderJson) {
				picked.countDown(); // the relay has selected the order and is about to send it
				awaitLatch(proceed);
				return super.publish(eventId, orderId, orderJson);
			}
		});

		try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
			Future<Integer> relaying = pool.submit(() -> pausedRelay.relay(eventId));
			assertThat(picked.await(10, TimeUnit.SECONDS)).isTrue();

			// The request now concludes that its own send never left the producer.
			assertThat(stock.releaseAndForget(eventId, orderId, user, "k1")).isFalse();

			proceed.countDown();
			assertThat(relaying.get()).isOne();
		}

		awaitOrder(orderId, OrderStatus.PENDING_PAYMENT);
		awaitStoresAgree(eventId);
		assertThat(stock.available(eventId)).hasValue(8);
	}

	@Test
	void aNotSentReleaseThatCameFirstMeansTheRelaySendsNothing() {
		long eventId = createEvent(10, 4);
		String user = user("release-first");
		String orderId = reserveInRedis(eventId, user, 2, "k1");

		assertThat(stock.releaseAndForget(eventId, orderId, user, "k1")).isTrue();

		assertThat(relay.relay(eventId)).isZero();
		assertThat(orders.existsById(orderId)).isFalse();
		assertThat(stock.available(eventId)).hasValue(10);
		assertStoresAgree(eventId);
	}

	@Test
	void anOrderTheConsumerAlreadyClaimedCannotBeReleasedAsNotSent() {
		long eventId = createEvent(10, 4);
		String user = user("consumer-first");
		String orderId = UUID.randomUUID().toString();
		String message = message(orderId, eventId, user, 2, "k1");
		stock.reserve(eventId, user, 2, 4, "k1", orderId, message, SALE_OPEN);

		consumer.onOrderRequested(message); // Kafka delivered it before the request gave up on its send

		assertThat(stock.releaseAndForget(eventId, orderId, user, "k1")).isFalse();
		assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.PENDING_PAYMENT);
		assertStoresAgree(eventId);
	}

	@Test
	void ifNotSentWasWrongTheReleasedOrderIsRejectedInsteadOfActivated() {
		long eventId = createEvent(10, 4);
		String user = user("misclassified");
		// The broker got the record, but the request believed it was never sent.
		ReservationService service = serviceWith(new OrderRequestPublisher(kafka, properties) {
			@Override
			public Delivery publish(long eventId, String orderId, String orderJson) {
				super.publish(eventId, orderId, orderJson);
				return Delivery.NOT_SENT;
			}
		});

		// Consumer held back so the request's release deterministically lands before the claim.
		withConsumerStopped(() -> assertThatThrownBy(() -> service.reserve(eventId, user, 2, "k1"))
			.isInstanceOf(ApiException.class)
			.hasFieldOrPropertyWithValue("status", HttpStatus.SERVICE_UNAVAILABLE));

		await().atMost(Duration.ofSeconds(15)).until(() -> !orders.findByEventId(eventId).isEmpty());
		assertThat(orders.findByEventId(eventId)).singleElement()
			.satisfies(order -> assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED));
		awaitStoresAgree(eventId);
		assertThat(stock.available(eventId)).hasValue(10);
		assertThat(eventRepository.findById(eventId).orElseThrow().getAvailableTickets()).isEqualTo(10);
	}

	@Test
	void aRequestWhoseNotSentReleaseIsRefusedStillAnswers202() {
		long eventId = createEvent(10, 4);
		String user = user("refused");
		// Stand-in for the relay picking the order between the reservation and the failed send.
		ReservationService service = serviceWith(new OrderRequestPublisher(kafka, properties) {
			@Override
			public Delivery publish(long eventId, String orderId, String orderJson) {
				stock.dueForRepublish(eventId, Duration.ZERO, 10);
				return Delivery.NOT_SENT;
			}
		});

		String orderId = service.reserve(eventId, user, 2, "k1").orderId();

		assertThat(relay.relay(eventId)).isOne();
		awaitOrder(orderId, OrderStatus.PENDING_PAYMENT);
		awaitStoresAgree(eventId);
	}

	private String user(String name) {
		return String.valueOf(userId(name));
	}

	private String reserveInRedis(long eventId, String user, int quantity, String key) {
		String orderId = UUID.randomUUID().toString();
		stock.reserve(eventId, user, quantity, 4, key, orderId, message(orderId, eventId, user, quantity, key), SALE_OPEN);
		return orderId;
	}

	private String message(String orderId, long eventId, String user, int quantity, String key) {
		return json.writeValueAsString(
				new OrderRequested(orderId, eventId, user, quantity, quantity * 2500, key, Instant.now()));
	}

	private void awaitOrder(String orderId, OrderStatus status) {
		await().atMost(Duration.ofSeconds(15))
			.until(() -> orders.findById(orderId).map(o -> o.getStatus() == status).orElse(false));
	}

	private OrderOutboxRelay relayWith(OrderRequestPublisher publisher) {
		return new OrderOutboxRelay(eventRepository, stock, publisher, properties, meters, Clock.systemUTC());
	}

	private ReservationService serviceWith(OrderRequestPublisher publisher) {
		return new ReservationService(catalog, stock, recovery, rateLimiter, publisher, pendingOrders, orders, json,
				meters, Clock.systemUTC());
	}

	private static void awaitLatch(CountDownLatch latch) {
		try {
			if (!latch.await(10, TimeUnit.SECONDS)) {
				throw new IllegalStateException("latch timed out");
			}
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(ex);
		}
	}

}
