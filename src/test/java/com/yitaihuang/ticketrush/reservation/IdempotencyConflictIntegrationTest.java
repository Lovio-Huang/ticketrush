package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

import com.yitaihuang.ticketrush.AbstractIntegrationTest;
import com.yitaihuang.ticketrush.common.ApiException;
import com.yitaihuang.ticketrush.order.OrderService;

/**
 * An Idempotency-Key belongs to one request: the same user, event and quantity.
 * Sent again with another quantity it is a conflict, never the original order
 * and never a second reservation, however the retry reaches us.
 */
class IdempotencyConflictIntegrationTest extends AbstractIntegrationTest {

	@Autowired MockMvcTester mvc;

	@Autowired StockGateway stock;

	@Autowired StockRecovery recovery;

	@Autowired OrderService orderService;

	@Autowired JdbcTemplate jdbc;

	@Autowired CacheManager caches;

	@Test
	void theSameKeyWithAnotherQuantityIsAConflictOverHttp() throws Exception {
		long eventId = createEvent(10, 4);
		String user = "idem-http";

		var first = post(eventId, user, "k1", 1);
		assertThat(first).hasStatus(HttpStatus.ACCEPTED);
		String orderId = json.readTree(first.getResponse().getContentAsString()).get("orderId").asString();

		assertThat(post(eventId, user, "k1", 2)).hasStatus(HttpStatus.UNPROCESSABLE_ENTITY)
			.bodyJson()
			.extractingPath("$.code")
			.isEqualTo("IDEMPOTENCY_CONFLICT");
		var same = post(eventId, user, "k1", 1);
		assertThat(same).hasStatus(HttpStatus.ACCEPTED).hasHeader("Idempotent-Replayed", "true");
		assertThat(json.readTree(same.getResponse().getContentAsString()).get("orderId").asString()).isEqualTo(orderId);

		await().atMost(Duration.ofSeconds(15)).until(() -> orders.existsById(orderId));
		assertThat(stock.available(eventId)).hasValue(9);
		awaitStoresAgree(eventId);
	}

	@Test
	void concurrentRequestsWithOneKeyReserveOnceAndConflictingOnesAreRefused() throws Exception {
		long eventId = createEvent(100, 10);
		String user = String.valueOf(userId("idem-race"));
		CountDownLatch start = new CountDownLatch(1);
		List<Future<Object>> calls = new ArrayList<>();
		try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
			for (int i = 0; i < 8; i++) { // within one user's rate limit (10)
				int quantity = 1 + i % 2;
				calls.add(pool.submit(() -> {
					start.await();
					try {
						return reservations.reserve(eventId, user, quantity, "race") + "|" + quantity;
					}
					catch (ApiException ex) {
						return ex.getCode() + "|" + quantity;
					}
				}));
			}
			start.countDown();
		}
		List<String> results = calls.stream().map(Future::resultNow).map(String::valueOf).toList();

		List<String> fresh = results.stream().filter(r -> r.contains("replayed=false")).toList();
		assertThat(fresh).as(results.toString()).hasSize(1);
		String winnerQuantity = fresh.get(0).substring(fresh.get(0).lastIndexOf('|') + 1);
		for (String result : results) {
			if (result.endsWith("|" + winnerQuantity)) {
				assertThat(result).as("same request").contains("Result[orderId=");
			}
			else {
				assertThat(result).as("other quantity").startsWith("IDEMPOTENCY_CONFLICT|");
			}
		}
		Set<String> orderIds = results.stream()
			.filter(r -> r.startsWith("Result"))
			.map(r -> r.substring(r.indexOf('=') + 1, r.indexOf(',')))
			.collect(Collectors.toSet());
		assertThat(orderIds).hasSize(1);
		assertThat(stock.available(eventId)).hasValue(100 - Integer.parseInt(winnerQuantity));
		awaitStoresAgree(eventId);
	}

	@Test
	void anotherUserOrEventWithTheSameKeyIsANewRequest() {
		long eventId = createEvent(10, 4);
		long otherEvent = createEvent(10, 4);
		String alice = String.valueOf(userId("idem-scope-a"));
		String bob = String.valueOf(userId("idem-scope-b"));

		String first = reservations.reserve(eventId, alice, 1, "shared").orderId();
		String bobs = reservations.reserve(eventId, bob, 2, "shared").orderId();
		String elsewhere = reservations.reserve(otherEvent, alice, 2, "shared").orderId();

		assertThat(Set.of(first, bobs, elsewhere)).hasSize(3);
		assertThat(stock.available(eventId)).hasValue(7);
		assertThat(stock.available(otherEvent)).hasValue(8);
	}

	@Test
	void afterTheSaleEndsARetryWithAnotherQuantityIsAConflict() {
		long eventId = createEvent(10, 4);
		String user = String.valueOf(userId("idem-closed"));
		String orderId = reservations.reserve(eventId, user, 1, "k1").orderId();
		jdbc.update("update events set starts_at = ? where id = ?",
				Instant.now().minusSeconds(1).atOffset(ZoneOffset.UTC).toLocalDateTime(), eventId);
		caches.getCache("events").evict(eventId);

		assertThatThrownBy(() -> reservations.reserve(eventId, user, 2, "k1"))
			.hasFieldOrPropertyWithValue("code", "IDEMPOTENCY_CONFLICT");
		assertThat(reservations.reserve(eventId, user, 1, "k1").orderId()).isEqualTo(orderId);
	}

	@Test
	void afterARedisRebuildTheKeyStillRemembersItsQuantity() {
		long eventId = createEvent(10, 4);
		String user = String.valueOf(userId("idem-rebuild"));
		String orderId = reservations.reserve(eventId, user, 2, "k1").orderId();
		await().atMost(Duration.ofSeconds(15)).until(() -> orders.existsById(orderId));
		wipeRedis(eventId);
		quietPeriodOver(eventId);
		assertThat(recovery.rebuild(eventId)).isTrue();

		assertThatThrownBy(() -> reservations.reserve(eventId, user, 1, "k1"))
			.hasFieldOrPropertyWithValue("code", "IDEMPOTENCY_CONFLICT");
		assertThat(reservations.reserve(eventId, user, 2, "k1").orderId()).isEqualTo(orderId);
		assertThat(stock.available(eventId)).hasValue(8);
	}

	@Test
	void aKeyWrittenBeforeQuantitiesWereRecordedIsCheckedAgainstTheOrder() {
		long eventId = createEvent(10, 4);
		String user = String.valueOf(userId("idem-legacy"));
		String held = reservations.reserve(eventId, user, 1, "held").orderId();
		String cancelled = reservations.reserve(eventId, user, 2, "cancelled").orderId();
		await().atMost(Duration.ofSeconds(15)).until(() -> orders.existsById(held) && orders.existsById(cancelled));
		orderService.cancel(cancelled, user); // released in Redis: only MySQL still knows its quantity
		// Redis data from before this version: the key holds the order id alone.
		String prefix = "ticketrush:{event:" + eventId + "}:idem:" + user + ":";
		redis.opsForValue().set(prefix + "held", held);
		redis.opsForValue().set(prefix + "cancelled", cancelled);

		assertThatThrownBy(() -> reservations.reserve(eventId, user, 3, "held"))
			.hasFieldOrPropertyWithValue("code", "IDEMPOTENCY_CONFLICT");
		assertThat(reservations.reserve(eventId, user, 1, "held").orderId()).isEqualTo(held);
		assertThatThrownBy(() -> reservations.reserve(eventId, user, 1, "cancelled"))
			.hasFieldOrPropertyWithValue("code", "IDEMPOTENCY_CONFLICT");
		assertThat(reservations.reserve(eventId, user, 2, "cancelled").orderId()).isEqualTo(cancelled);
		assertThat(stock.available(eventId)).hasValue(9);
	}

	private org.springframework.test.web.servlet.assertj.MvcTestResult post(long eventId, String user, String key,
			int quantity) {
		return mvc.post()
			.uri("/api/events/{id}/reservations", eventId)
			.header("Authorization", bearer(user))
			.header("Idempotency-Key", key)
			.contentType(MediaType.APPLICATION_JSON)
			.content("{\"quantity\":" + quantity + "}")
			.exchange();
	}

}
