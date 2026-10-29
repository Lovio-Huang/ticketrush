package com.yitaihuang.ticketrush.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

import com.yitaihuang.ticketrush.AbstractIntegrationTest;
import com.yitaihuang.ticketrush.reservation.StockGateway;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class OrderHistoryAndStatsIntegrationTest extends AbstractIntegrationTest {

	@Autowired
	MockMvcTester mvc;

	@Autowired
	JsonMapper json;

	@Autowired
	OrderService orderService;

	@Autowired
	StockGateway stock;

	private JsonNode get(String uri, String authorization) throws Exception {
		var result = mvc.get().uri(uri).header("Authorization", authorization).exchange();
		assertThat(result).hasStatusOk();
		return json.readTree(result.getResponse().getContentAsByteArray());
	}

	private static List<String> orderIds(JsonNode page) {
		List<String> ids = new ArrayList<>();
		page.get("items").forEach(item -> ids.add(item.get("orderId").asString()));
		return ids;
	}

	@Test
	void myOrdersAreNewestFirstFilterableAndPaged() throws Exception {
		String me = String.valueOf(userId("hanna"));
		long concert = createEvent(10, 4);
		long play = createEvent(10, 4);
		String first = reserveAndAwaitOrder(concert, me, 1);
		String second = reserveAndAwaitOrder(play, me, 2);
		String third = reserveAndAwaitOrder(concert, me, 1);
		orderService.pay(second, me);
		reserveAndAwaitOrder(concert, String.valueOf(userId("ivan")), 1); // someone else's

		JsonNode all = get("/api/orders", bearer("hanna"));
		assertThat(orderIds(all)).containsExactly(third, second, first);
		assertThat(all.get("totalItems").asLong()).isEqualTo(3);
		assertThat(orderIds(get("/api/orders?status=PAID", bearer("hanna")))).containsExactly(second);

		JsonNode firstPage = get("/api/orders?size=2", bearer("hanna"));
		assertThat(orderIds(firstPage)).containsExactly(third, second);
		assertThat(firstPage.get("totalPages").asInt()).isEqualTo(2);
		assertThat(orderIds(get("/api/orders?size=2&page=1", bearer("hanna")))).containsExactly(first);
	}

	@Test
	void aBadFilterOrPageSizeIsA400WithACode() {
		assertThat(mvc.get().uri("/api/orders?status=SHIPPED").header("Authorization", bearer("hanna")))
			.hasStatus(HttpStatus.BAD_REQUEST);
		assertThat(mvc.get().uri("/api/orders?size=101").header("Authorization", bearer("hanna")))
			.hasStatus(HttpStatus.BAD_REQUEST)
			.bodyJson()
			.extractingPath("$.code")
			.isEqualTo("INVALID_REQUEST");
	}

	@Test
	void salesStatsAccountForEveryTicketAndCompareTheTwoStores() throws Exception {
		long eventId = createEvent(10, 4);
		String paid = reserveAndAwaitOrder(eventId, String.valueOf(userId("judy")), 3);
		orderService.pay(paid, String.valueOf(userId("judy")));
		reserveAndAwaitOrder(eventId, String.valueOf(userId("karl")), 2); // still pending

		JsonNode stats = get("/api/events/" + eventId + "/stats", adminBearer());
		assertThat(stats.get("availableInDatabase").asInt()).isEqualTo(5);
		assertThat(stats.get("availableInRedis").asInt()).isEqualTo(5);
		assertThat(stats.get("storesInSync").asBoolean()).isTrue();
		assertThat(stats.get("ticketsSold").asLong()).isEqualTo(3);
		assertThat(stats.get("revenueCents").asLong()).isEqualTo(3 * 2500);
		assertThat(stats.at("/byStatus/PENDING_PAYMENT/tickets").asLong()).isEqualTo(2);
		assertThat(stats.at("/byStatus/EXPIRED/orders").asLong()).isZero();

		stock.initialize(eventId, 7); // Redis drifts
		JsonNode drifted = get("/api/events/" + eventId + "/stats", adminBearer());
		assertThat(drifted.get("availableInRedis").asInt()).isEqualTo(7);
		assertThat(drifted.get("storesInSync").asBoolean()).isFalse();
	}

	@Test
	void upcomingEventsArePublicAndShowRedisLiveAvailability() throws Exception {
		long eventId = createEvent(10, 4);
		stock.initialize(eventId, 3); // as if 7 tickets were reserved and not yet persisted

		JsonNode found = null;
		for (int page = 0; found == null; page++) {
			var result = mvc.get().uri("/api/events?size=50&page=" + page).exchange(); // no token
			assertThat(result).hasStatusOk();
			JsonNode body = json.readTree(result.getResponse().getContentAsByteArray());
			for (JsonNode event : body.get("items")) {
				if (event.get("id").asLong() == eventId) {
					found = event;
				}
			}
			assertThat(page).as("event %d is listed", eventId).isLessThan(body.get("totalPages").asInt());
		}
		assertThat(found.get("availableTickets").asInt()).isEqualTo(3);
		assertThat(mvc.get().uri("/api/events?size=51")).hasStatus(HttpStatus.BAD_REQUEST);
	}

}
