package com.yitaihuang.ticketrush;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.testcontainers.junit.jupiter.Testcontainers;

import com.yitaihuang.ticketrush.auth.Role;
import com.yitaihuang.ticketrush.auth.TokenService;
import com.yitaihuang.ticketrush.event.CreateEventRequest;
import com.yitaihuang.ticketrush.event.EventRepository;
import com.yitaihuang.ticketrush.event.EventService;
import com.yitaihuang.ticketrush.order.Order;
import com.yitaihuang.ticketrush.order.OrderRepository;
import com.yitaihuang.ticketrush.order.OrderRequested;
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
		"ticketrush.reservations.relay-interval=PT1H", "ticketrush.reservations.republish-after=PT0S" })
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

	/** Waits until nothing is in flight and both stores tell the same story (see {@link #assertStoresAgree}). */
	protected void awaitStoresAgree(long eventId) {
		await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertStoresAgree(eventId));
	}

	/**
	 * The invariants, checked together: Redis stock and per-user counts equal what
	 * the held orders imply, MySQL stock equals what its active orders imply, Redis
	 * holds exactly MySQL's active orders, the outbox is empty, and every stored
	 * order's idempotency key still points at it.
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
		assertThat(held).as("orders Redis holds vs active orders in MySQL").isEqualTo(active);
		assertThat(redis.opsForValue().get(prefix + "stock")).as("Redis stock")
			.isEqualTo(String.valueOf(total - sum(held)));
		assertThat(buyers).as("per-user counts").isEqualTo(perUser);
		assertThat(eventRepository.findById(eventId).orElseThrow().getAvailableTickets()).as("MySQL stock")
			.isEqualTo(total - sum(active));
		for (Order order : stored) {
			assertThat(redis.opsForValue().get(prefix + "idem:" + order.getUserId() + ":" + order.getIdempotencyKey()))
				.as("idempotency key of %s", order.getId())
				.isEqualTo(order.getId());
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
