package com.yitaihuang.ticketrush.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

import com.yitaihuang.ticketrush.AbstractIntegrationTest;
import com.yitaihuang.ticketrush.order.OrderFailure.Status;
import com.yitaihuang.ticketrush.reservation.StockGateway;

/**
 * What a customer sees when polling an order the consumer dead-lettered: its
 * own order is never a 404, someone else's always is, and nothing internal
 * leaks.
 */
class OrderStatusIntegrationTest extends AbstractIntegrationTest {

	@Autowired MockMvcTester mvc;

	@Autowired OrderFailureService failures;

	@Autowired StockGateway stock;

	@Test
	void aDeadLetteredOrderStaysVisibleToItsOwnerAfterTheProcessingMarkExpires() throws Exception {
		long eventId = createEvent(10, 4);
		String owner = "status-owner";
		String orderId = UUID.randomUUID().toString();
		String message = reserveInRedis(eventId, orderId, String.valueOf(userId(owner)), 2);
		failures.record(message, "java.sql.SQLException: something internal", "test-status-" + orderId + "@1");
		redis.delete("ticketrush:order:" + orderId + ":processing"); // the 10-minute mark ran out

		var seen = mvc.get().uri("/api/orders/{id}", orderId).header("Authorization", bearer(owner)).exchange();
		assertThat(seen).hasStatusOk();
		assertThat(seen).bodyJson().extractingPath("$.status").isEqualTo("UNDER_REVIEW");
		assertThat(seen).bodyJson().extractingPath("$.quantity").isEqualTo(2);
		assertThat(seen.getResponse().getContentAsString()).doesNotContain("SQLException", "payload", "error", "note",
				"attempts");

		assertThat(mvc.get().uri("/api/orders/{id}", orderId).header("Authorization", bearer("status-mallory")))
			.hasStatus(HttpStatus.NOT_FOUND)
			.bodyJson()
			.extractingPath("$.code")
			.isEqualTo("ORDER_NOT_FOUND");
	}

	@Test
	void aDeadLetterWhoseOwnerCantBeEstablishedIsShownToNoOne() {
		String owner = "status-unreadable";
		String orderId = UUID.randomUUID().toString();
		// Names an order and a user, but isn't a complete order message.
		String payload = "{\"orderId\":\"" + orderId + "\",\"userId\":\"" + userId(owner) + "\"}";
		failures.record(payload, "boom", "test-status-unreadable-" + orderId + "@1");

		assertThat(mvc.get().uri("/api/orders/{id}", orderId).header("Authorization", bearer(owner)))
			.hasStatus(HttpStatus.NOT_FOUND);
	}

	@Test
	void replayingThenStoringTheOrderMovesItsStatusAlong() throws Exception {
		long eventId = createEvent(10, 4);
		String owner = "status-replay";
		String orderId = UUID.randomUUID().toString();
		String message = reserveInRedis(eventId, orderId, String.valueOf(userId(owner)), 1);
		failures.record(message, "boom", "test-status-replay-" + orderId + "@1");
		long failureId = failureOf(orderId).getId();

		assertThat(statusOf(orderId, owner)).isEqualTo("UNDER_REVIEW");
		failures.replay(failureId, null);
		await().atMost(Duration.ofSeconds(15)).until(() -> orders.existsById(orderId));
		assertThat(statusOf(orderId, owner)).isEqualTo("PENDING_PAYMENT");
		awaitStoresAgree(eventId);
	}

	@Test
	void aVoidedOrderShowsAsRejected() throws Exception {
		long eventId = createEvent(10, 4);
		String owner = "status-void";
		String orderId = UUID.randomUUID().toString();
		String message = reserveInRedis(eventId, orderId, String.valueOf(userId(owner)), 1);
		failures.record(message, "boom", "test-status-void-" + orderId + "@1");

		failures.voidOrder(failureOf(orderId).getId(), "given up");

		assertThat(statusOf(orderId, owner)).isEqualTo("REJECTED");
		assertThat(failureOf(orderId).getStatus()).isEqualTo(Status.VOIDED);
	}

	private String statusOf(String orderId, String user) throws Exception {
		var response = mvc.get().uri("/api/orders/{id}", orderId).header("Authorization", bearer(user)).exchange();
		assertThat(response).hasStatusOk();
		return json.readTree(response.getResponse().getContentAsString()).get("status").asString();
	}

	private OrderFailure failureOf(String orderId) {
		return failures.list(null).stream().filter(f -> orderId.equals(f.getOrderId())).findFirst().orElseThrow();
	}

	private String reserveInRedis(long eventId, String orderId, String user, int quantity) {
		String message = json.writeValueAsString(
				new OrderRequested(orderId, eventId, user, quantity, quantity * 2500, "k1", Instant.now()));
		stock.reserve(eventId, user, quantity, 4, "k1", orderId, message, SALE_OPEN);
		return message;
	}

}
