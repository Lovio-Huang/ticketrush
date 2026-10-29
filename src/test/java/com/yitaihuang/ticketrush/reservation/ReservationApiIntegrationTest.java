package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import com.yitaihuang.ticketrush.AbstractIntegrationTest;
import com.yitaihuang.ticketrush.order.OrderRepository;

import tools.jackson.databind.json.JsonMapper;

class ReservationApiIntegrationTest extends AbstractIntegrationTest {

	@Autowired
	MockMvcTester mvc;

	@Autowired
	JsonMapper json;

	@Autowired
	OrderRepository orders;

	@Test
	void reserveThenPay() {
		long eventId = createEvent(10, 4);

		MvcTestResult reserved = reserve(eventId, "alice", "checkout-1", 2);
		assertThat(reserved).hasStatus(HttpStatus.ACCEPTED).hasHeader("Idempotent-Replayed", "false");
		String orderId = orderId(reserved);

		await().atMost(Duration.ofSeconds(15))
			.untilAsserted(() -> assertThat(
					mvc.get().uri("/api/orders/{id}", orderId).header("Authorization", bearer("alice")))
				.bodyJson()
				.extractingPath("$.status")
				.isEqualTo("PENDING_PAYMENT"));

		assertThat(mvc.post().uri("/api/orders/{id}/payment", orderId).header("Authorization", bearer("alice")))
			.hasStatusOk()
			.bodyJson()
			.extractingPath("$.status")
			.isEqualTo("PAID");
		assertThat(mvc.post().uri("/api/orders/{id}/payment", orderId).header("Authorization", bearer("alice")))
			.hasStatus(HttpStatus.CONFLICT)
			.bodyJson()
			.extractingPath("$.code")
			.isEqualTo("INVALID_ORDER_STATE");
	}

	@Test
	void retryingWithTheSameIdempotencyKeyReturnsTheOriginalOrder() {
		long eventId = createEvent(10, 4);

		String first = orderId(reserve(eventId, "bob", "retry-me", 1));
		MvcTestResult retry = reserve(eventId, "bob", "retry-me", 1);

		assertThat(retry).hasStatus(HttpStatus.ACCEPTED).hasHeader("Idempotent-Replayed", "true");
		assertThat(orderId(retry)).isEqualTo(first);
		await().atMost(Duration.ofSeconds(15)).until(() -> orders.existsById(first));
		assertThat(eventService.get(eventId).availableTickets()).isEqualTo(9);
	}

	@Test
	void ordersAreInvisibleToOtherUsers() {
		long eventId = createEvent(10, 4);
		String orderId = orderId(reserve(eventId, "carol", "mine", 1));
		await().atMost(Duration.ofSeconds(15)).until(() -> orders.existsById(orderId));

		assertThat(mvc.get().uri("/api/orders/{id}", orderId).header("Authorization", bearer("mallory")))
			.hasStatus(HttpStatus.NOT_FOUND);
	}

	@Test
	void rejectsRequestsBeforeTheSaleOpens() {
		Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
		String body = """
				{"name":"Later","venue":"Arena","startsAt":"%s","saleStartsAt":"%s",
				 "totalTickets":10,"priceCents":100,"perUserLimit":2}
				""".formatted(now.plus(30, ChronoUnit.DAYS), now.plus(1, ChronoUnit.DAYS));
		MvcTestResult created = mvc.post()
			.uri("/api/events")
			.header("Authorization", adminBearer())
			.contentType(MediaType.APPLICATION_JSON)
			.content(body)
			.exchange();
		assertThat(created).hasStatus(HttpStatus.CREATED);
		long eventId = json.readTree(created.getResponse().getContentAsByteArray()).get("id").asLong();

		assertThat(reserve(eventId, "dave", "early", 1)).hasStatus(HttpStatus.CONFLICT)
			.bodyJson()
			.extractingPath("$.code")
			.isEqualTo("SALE_NOT_STARTED");
	}

	@Test
	void rateLimitsAUserWhoHammersTheEndpoint() {
		long eventId = createEvent(1_000, 10);

		int throttled = 0;
		for (int i = 0; i < 20; i++) {
			if (reserve(eventId, "bot", "spam-" + i, 1).getResponse().getStatus() == 429) {
				throttled++;
			}
		}
		assertThat(throttled).isPositive();
	}

	@Test
	void validatesInput() {
		long eventId = createEvent(10, 4);

		assertThat(reserve(eventId, "erin", "bad-qty", 0)).hasStatus(HttpStatus.BAD_REQUEST);
		assertThat(mvc.post()
			.uri("/api/events/{id}/reservations", eventId)
			.header("Authorization", bearer("erin"))
			.contentType(MediaType.APPLICATION_JSON)
			.content("{\"quantity\":1}")).hasStatus(HttpStatus.BAD_REQUEST);
		assertThat(reserve(999_999, "erin", "no-event", 1)).hasStatus(HttpStatus.NOT_FOUND)
			.bodyJson()
			.extractingPath("$.code")
			.isEqualTo("EVENT_NOT_FOUND");
	}

	private MvcTestResult reserve(long eventId, String userId, String idempotencyKey, int quantity) {
		return mvc.post()
			.uri("/api/events/{id}/reservations", eventId)
			.header("Authorization", bearer(userId))
			.header("Idempotency-Key", idempotencyKey)
			.contentType(MediaType.APPLICATION_JSON)
			.content("{\"quantity\":" + quantity + "}")
			.exchange();
	}

	private String orderId(MvcTestResult result) {
		return json.readTree(result.getResponse().getContentAsByteArray()).get("orderId").asString();
	}

}
