package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.yitaihuang.ticketrush.TestcontainersConfiguration;
import com.yitaihuang.ticketrush.auth.Role;
import com.yitaihuang.ticketrush.auth.TokenService;
import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.event.CreateEventRequest;
import com.yitaihuang.ticketrush.event.EventService;
import com.yitaihuang.ticketrush.order.OrderFailureService;
import com.yitaihuang.ticketrush.order.OrderRepository;
import com.yitaihuang.ticketrush.order.OrderRequested;

import tools.jackson.databind.json.JsonMapper;

/**
 * The background jobs as they run in production: virtual threads, the default
 * scheduler, every @Scheduled job on, against containers of its own (closed
 * after this class). Whatever the other integration tests switch off or inject
 * at a boundary, these let run for real: a relay stuck on Kafka, a Redis check
 * that can't finish, a Redis that stops answering.
 */
@SpringBootTest(properties = "ticketrush.reservations.republish-after=PT0S") // the relay picks orders up at once
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RealSchedulingIntegrationTest {

	@MockitoSpyBean StockGateway stock;

	@MockitoSpyBean OrderRequestPublisher publisher;

	@Autowired RedisRestartGuard guard;

	@Autowired StockRecovery recovery;

	@Autowired ReservationService reservations;

	@Autowired EventService eventService;

	@Autowired OrderRepository orders;

	@Autowired OrderFailureService failures;

	@Autowired OrderQueueLag queueLag;

	@Autowired KafkaTemplate<String, String> kafka;

	@Autowired TicketrushProperties properties;

	@Autowired TokenService tokens;

	@Autowired JsonMapper json;

	@Autowired MockMvcTester mvc;

	@Autowired @Qualifier("redisContainer") GenericContainer<?> redisContainer;

	private static final AtomicInteger NEXT_USER = new AtomicInteger(5_000_000);

	@Test
	void aRelayStuckOnKafkaDoesNotStopTheRedisCheck() throws Exception {
		long eventId = readyEvent();
		String orderId = UUID.randomUUID().toString();
		CountDownLatch relayStuck = new CountDownLatch(1);
		CountDownLatch unstick = new CountDownLatch(1);
		doAnswer(call -> {
			relayStuck.countDown();
			unstick.await(60, TimeUnit.SECONDS); // Kafka not answering
			return call.callRealMethod();
		}).when(publisher).publish(anyLong(), eq(orderId), anyString());
		AtomicInteger checks = new AtomicInteger();
		doAnswer(call -> {
			checks.incrementAndGet();
			return call.callRealMethod();
		}).when(stock).checkServer(anyString(), any(), anyString());
		// Accepted, never confirmed by Kafka: the relay will send it, and get stuck.
		String user = nextUser();
		stock.reserve(eventId, user, 1, 4, "k1", orderId, message(orderId, eventId, user, 1),
				Instant.now().plus(1, ChronoUnit.DAYS));

		try {
			assertThat(relayStuck.await(20, TimeUnit.SECONDS)).as("the relay picked the order up").isTrue();
			int before = checks.get();
			await().atMost(Duration.ofSeconds(10)).until(() -> checks.get() >= before + 3);
			assertThat(guard.verified()).isTrue();
			assertThat(reservations.reserve(eventId, nextUser(), 1, "k1").replayed()).isFalse(); // sales go on
		}
		finally {
			unstick.countDown();
		}
		await().atMost(Duration.ofSeconds(30)).until(() -> orders.existsById(orderId));
	}

	@Test
	void whileTheRedisCheckCantFinishItsVerdictExpiresAndSalesStop() throws Exception {
		long eventId = readyEvent();
		CountDownLatch checkStuck = new CountDownLatch(1);
		CountDownLatch unstick = new CountDownLatch(1);
		doAnswer(call -> {
			checkStuck.countDown();
			unstick.await(60, TimeUnit.SECONDS); // e.g. the process or its network paused mid-check
			return call.callRealMethod();
		}).when(stock).serverRunId();

		try {
			assertThat(checkStuck.await(10, TimeUnit.SECONDS)).isTrue();
			await().atMost(Duration.ofSeconds(10)).until(() -> !guard.verified());
			int stockBefore = stock.available(eventId).orElseThrow();
			long outboxBefore = stock.outboxSize(eventId);

			assertThatThrownBy(() -> reservations.reserve(eventId, nextUser(), 1, "k1"))
				.hasFieldOrPropertyWithValue("code", "STOCK_RECOVERING");
			assertThat(recovery.rebuild(eventId)).isFalse();
			assertThat(stock.available(eventId)).hasValue(stockBefore);
			assertThat(stock.outboxSize(eventId)).isEqualTo(outboxBefore);
		}
		finally {
			unstick.countDown();
		}
		await().atMost(Duration.ofSeconds(10)).until(guard::verified);
		assertThat(reservations.reserve(eventId, nextUser(), 1, "k1").replayed()).isFalse();
	}

	@Test
	void aRedisThatStopsAnsweringIsA503WithinTheCommandTimeout() throws Exception {
		long eventId = readyEvent();
		String user = nextUser();

		pauseRedis();
		MvcTestResult refused;
		long started = System.nanoTime();
		try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
			refused = pool.submit(() -> post(eventId, user, "k1", 1)).get(15, TimeUnit.SECONDS);
		}
		finally {
			unpauseRedis();
		}

		assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(10));
		assertThat(refused).hasStatus(HttpStatus.SERVICE_UNAVAILABLE)
			.bodyJson()
			.extractingPath("$.code")
			.isEqualTo("TEMPORARILY_UNAVAILABLE");
		await().atMost(Duration.ofSeconds(10)).until(guard::verified);
		assertThat(post(eventId, user, "k1", 1)).hasStatus(HttpStatus.ACCEPTED);
	}

	@Test
	void aReservationThatTimedOutButRanIsReturnedToTheRetryNotMadeTwice() throws Exception {
		long eventId = readyEvent();
		String user = nextUser();
		// Redis stops answering just as the reservation script is sent: the request times out, but the
		// script is still in Redis's socket and runs as soon as Redis is back.
		AtomicBoolean armed = new AtomicBoolean(true);
		doAnswer(call -> {
			if (armed.compareAndSet(true, false)) {
				pauseRedis();
			}
			return call.callRealMethod();
		}).when(stock).reserve(anyLong(), anyString(), anyInt(), anyInt(), anyString(), anyString(), anyString(), any());

		MvcTestResult timedOut;
		try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
			timedOut = pool.submit(() -> post(eventId, user, "k1", 1)).get(15, TimeUnit.SECONDS);
		}
		finally {
			unpauseRedis();
		}
		assertThat(timedOut).hasStatus(HttpStatus.SERVICE_UNAVAILABLE);
		await().atMost(Duration.ofSeconds(10)).until(() -> stock.available(eventId).orElse(-1) == 9);

		await().atMost(Duration.ofSeconds(10)).until(guard::verified);
		MvcTestResult retry = post(eventId, user, "k1", 1);
		assertThat(retry).hasStatus(HttpStatus.ACCEPTED).hasHeader("Idempotent-Replayed", "true");
		String orderId = json.readTree(retry.getResponse().getContentAsString()).get("orderId").asString();
		await().atMost(Duration.ofSeconds(30)).until(() -> orders.existsById(orderId));
		assertThat(orders.findByEventId(eventId)).hasSize(1);
		assertThat(stock.available(eventId)).hasValue(9);
	}

	@Test
	void theConsumerRidesOutAShortRedisOutageWithoutDeadLetteringTheOrder() {
		long eventId = readyEvent();
		String user = nextUser();
		String orderId = UUID.randomUUID().toString();
		String message = message(orderId, eventId, user, 1);
		AtomicInteger claims = new AtomicInteger();
		doAnswer(call -> {
			if (orderId.equals(call.getArgument(1))) {
				claims.incrementAndGet();
			}
			return call.callRealMethod();
		}).when(stock).confirm(anyLong(), anyString(), anyString(), anyString(), anyString(), any(), anyBoolean());
		stock.reserve(eventId, user, 1, 4, "k1", orderId, message, Instant.now().plus(1, ChronoUnit.DAYS));

		pauseRedis();
		try {
			kafka.send(properties.orders().topic(), String.valueOf(eventId), message).join();
			// Each delivery claims twice (the batch, then the order alone). The old budget, 4 attempts 0.5 s
			// apart, ends at claim 8 with a dead letter; a 9th claim exists only if the budget outlasts it.
			await().atMost(Duration.ofSeconds(40)).until(() -> claims.get() >= 9);
		}
		finally {
			unpauseRedis();
		}

		await().atMost(Duration.ofSeconds(60)).until(() -> orders.existsById(orderId));
		assertThat(failures.list(null)).noneMatch(f -> orderId.equals(f.getOrderId()));
		await().atMost(Duration.ofSeconds(30)).until(() -> queueLag.current().orElse(-1) == 0); // offsets committed
	}

	private long readyEvent() {
		await().atMost(Duration.ofSeconds(15)).until(guard::verified);
		Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
		return eventService.create(new CreateEventRequest("Scheduling test", "Test Arena",
				now.plus(30, ChronoUnit.DAYS), now.minus(1, ChronoUnit.MINUTES), 10, 2500, 4))
			.id();
	}

	private String message(String orderId, long eventId, String user, int quantity) {
		return json.writeValueAsString(
				new OrderRequested(orderId, eventId, user, quantity, quantity * 2500, "k1", Instant.now()));
	}

	private static String nextUser() {
		return String.valueOf(NEXT_USER.incrementAndGet());
	}

	private MvcTestResult post(long eventId, String user, String key, int quantity) {
		String token = tokens.issue(Long.parseLong(user), "user-" + user, Role.USER).accessToken();
		return mvc.post()
			.uri("/api/events/{id}/reservations", eventId)
			.header("Authorization", "Bearer " + token)
			.header("Idempotency-Key", key)
			.contentType(MediaType.APPLICATION_JSON)
			.content("{\"quantity\":" + quantity + "}")
			.exchange();
	}

	private void pauseRedis() {
		DockerClientFactory.instance().client().pauseContainerCmd(redisContainer.getContainerId()).exec();
	}

	private void unpauseRedis() {
		DockerClientFactory.instance().client().unpauseContainerCmd(redisContainer.getContainerId()).exec();
	}

}
