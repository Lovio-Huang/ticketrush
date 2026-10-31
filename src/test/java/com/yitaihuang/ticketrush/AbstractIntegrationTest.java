package com.yitaihuang.ticketrush;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.yitaihuang.ticketrush.auth.Role;
import com.yitaihuang.ticketrush.auth.TokenService;
import com.yitaihuang.ticketrush.event.CreateEventRequest;
import com.yitaihuang.ticketrush.event.EventRepository;
import com.yitaihuang.ticketrush.event.EventService;
import com.yitaihuang.ticketrush.order.Order;
import com.yitaihuang.ticketrush.order.OrderRepository;
import com.yitaihuang.ticketrush.order.OrderRequested;
import com.yitaihuang.ticketrush.order.OrderStatus;
import com.yitaihuang.ticketrush.reservation.ReservationService;

import tools.jackson.databind.json.JsonMapper;

/**
 * Boots the whole application against real infrastructure. All subclasses share
 * one Spring context (and one set of containers) thanks to context caching.
 * Skipped automatically on machines without Docker; CI always has it.
 */
// Background jobs off: tests that need them call them directly, at a known moment.
// The relay resends anything unconfirmed when a test calls it, without waiting.
@SpringBootTest(properties = { "ticketrush.orders.expiry-scan-interval=PT1H", "ticketrush.reconciliation.interval=PT1H",
		"ticketrush.reservations.relay-interval=PT1H", "ticketrush.reservations.republish-after=PT0S",
		// Recovery waits until a test says the quiet period is over (quietPeriodOver).
		"ticketrush.reservations.recovery-quiet-period=PT1H",
		// The Redis restart check runs once at startup; tests that need it call it.
		"ticketrush.reservations.redis-check-interval=PT1H" })
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
public abstract class AbstractIntegrationTest {

	@Autowired
	protected EventService eventService;

	@Autowired
	protected StringRedisTemplate redis;

	@Autowired
	protected TokenService tokens;

	@Autowired
	protected ReservationService reservations;

	@Autowired
	protected OrderRepository orders;

	@Autowired
	protected EventRepository eventRepository;

	@Autowired
	protected JsonMapper json;

	@Autowired
	private KafkaListenerEndpointRegistry listeners;

	/** A sale end far enough away for orders reserved straight in Redis, as {@link #createEvent} events have. */
	protected static final Instant SALE_OPEN = Instant.now().plus(30, ChronoUnit.DAYS);

	/** Test users by name, each with a stable numeric id: the JWT subject the API sees. */
	private static final Map<String, Long> USER_IDS = new ConcurrentHashMap<>();

	private static final AtomicLong NEXT_USER_ID = new AtomicLong(1_000_000);

	@BeforeEach
	void resetRateLimits() {
		var keys = redis.keys("ticketrush:ratelimit:*");
		if (!keys.isEmpty()) {
			redis.delete(keys);
		}
	}

	/** An {@code Authorization} header value for a regular user, signed like a real login would be. */
	protected String bearer(String user) {
		return "Bearer " + tokens.issue(userId(user), user, Role.USER).accessToken();
	}

	protected String adminBearer() {
		return "Bearer " + tokens.issue(userId("admin"), "admin", Role.ADMIN).accessToken();
	}

	/** The id orders are stored under for that user. */
	protected static long userId(String user) {
		return USER_IDS.computeIfAbsent(user, name -> NEXT_USER_ID.incrementAndGet());
	}

	/** Reserves through the real pipeline (Redis, Kafka) and waits until the order is in MySQL. */
	protected String reserveAndAwaitOrder(long eventId, String userId, int quantity) {
		String orderId = reservations.reserve(eventId, userId, quantity, UUID.randomUUID().toString()).orderId();
		await().atMost(Duration.ofSeconds(15)).until(() -> orders.existsById(orderId));
		return orderId;
	}

	/** Runs {@code body} with the order writer stopped, so accepted orders pile up in Kafka. */
	protected void withConsumerStopped(Runnable body) {
		MessageListenerContainer writer = listeners.getListenerContainer("order-writer");
		writer.stop();
		try {
			body.run();
		}
		finally {
			writer.start();
		}
	}

	/** Redis lost everything it had about the event, as after a restart without persistence. */
	protected void wipeRedis(long eventId) {
		redis.delete(redis.keys("ticketrush:{event:" + eventId + "}:*"));
	}

	/** Pretends the loss was noticed long ago, so the recovery quiet period has passed. */
	protected void quietPeriodOver(long eventId) {
		redis.opsForValue().set("ticketrush:{event:" + eventId + "}:recovering-since", "0");
	}

	/** Waits until nothing is in flight and both stores tell the same story (see {@link #assertStoresAgree}). */
	protected void awaitStoresAgree(long eventId) {
		await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertStoresAgree(eventId));
	}

	/**
	 * The invariants, checked together: Redis stock and per-user counts equal what
	 * the held orders imply, MySQL stock equals what its active orders imply, Redis
	 * holds exactly MySQL's active orders, the outbox is empty, and every stored
	 * order's idempotency key still points at it (with its quantity).
	 */
	protected void assertStoresAgree(long eventId) {
		String prefix = "ticketrush:{event:" + eventId + "}:";
		int total = eventRepository.findById(eventId).orElseThrow().getTotalTickets();

		Map<String, Integer> held = new HashMap<>();
		Map<String, Long> perUser = new HashMap<>();
		redis.<String, String>opsForHash().entries(prefix + "orders").forEach((orderId, value) -> {
			if (!"R".equals(value)) {
				OrderRequested order = json.readValue(value, OrderRequested.class);
				held.put(orderId, order.quantity());
				perUser.merge(order.userId(), (long) order.quantity(), Long::sum);
			}
		});
		List<Order> stored = orders.findByEventId(eventId);
		Map<String, Integer> active = stored.stream()
			.filter(order -> order.getStatus().holdsTickets())
			.collect(Collectors.toMap(Order::getId, Order::getQuantity));
		Map<String, Long> buyers = new HashMap<>();
		redis.<String, String>opsForHash()
			.entries(prefix + "buyers")
			.forEach((user, count) -> buyers.put(user, Long.parseLong(count)));

		assertThat(redis.opsForZSet().size(prefix + "outbox")).as("unconfirmed orders").isZero();
		assertThat(redis.opsForSet().members(prefix + "sent")).as("orders marked sent are held")
			.allSatisfy(orderId -> assertThat(held).containsKey(orderId));
		assertThat(held).as("orders Redis holds vs active orders in MySQL").isEqualTo(active);
		assertThat(redis.opsForValue().get(prefix + "stock")).as("Redis stock")
			.isEqualTo(String.valueOf(total - sum(held)));
		assertThat(buyers).as("per-user counts").isEqualTo(perUser);
		assertThat(Integer.parseInt(Objects.requireNonNullElse(redis.opsForValue().get(prefix + "held"), "0")))
			.as("held counter").isEqualTo(sum(held));
		assertThat(eventRepository.findById(eventId).orElseThrow().getAvailableTickets()).as("MySQL stock")
			.isEqualTo(total - sum(active));
		// A rejected order never held tickets for the client; its key may have been forgotten for a retry.
		for (Order order : stored.stream().filter(o -> o.getStatus() != OrderStatus.REJECTED).toList()) {
			assertThat(redis.opsForValue().get(prefix + "idem:" + order.getUserId() + ":" + order.getIdempotencyKey()))
				.as("idempotency key of %s: the order and the quantity it was for", order.getId())
				.isEqualTo(order.getId() + "|" + order.getQuantity());
		}
	}

	private static int sum(Map<String, Integer> quantities) {
		return quantities.values().stream().mapToInt(Integer::intValue).sum();
	}

	protected long createEvent(int tickets, int perUserLimit) {
		Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
		return eventService.create(new CreateEventRequest("Integration test", "Test Arena",
				now.plus(30, ChronoUnit.DAYS), now.minus(1, ChronoUnit.MINUTES), tickets, 2500, perUserLimit))
			.id();
	}

}
