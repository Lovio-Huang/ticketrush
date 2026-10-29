package com.yitaihuang.ticketrush;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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
import com.yitaihuang.ticketrush.event.EventService;
import com.yitaihuang.ticketrush.order.OrderRepository;
import com.yitaihuang.ticketrush.reservation.ReservationService;

/**
 * Boots the whole application against real infrastructure. All subclasses share
 * one Spring context (and one set of containers) thanks to context caching.
 * Skipped automatically on machines without Docker; CI always has it.
 */
// Background jobs off: tests that need them call them directly, at a known moment.
@SpringBootTest(properties = { "ticketrush.orders.expiry-scan-interval=PT1H", "ticketrush.reconciliation.interval=PT1H" })
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

	protected long createEvent(int tickets, int perUserLimit) {
		Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
		return eventService.create(new CreateEventRequest("Integration test", "Test Arena",
				now.plus(30, ChronoUnit.DAYS), now.minus(1, ChronoUnit.MINUTES), tickets, 2500, perUserLimit))
			.id();
	}

}
