package com.yitaihuang.ticketrush.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.yitaihuang.ticketrush.AbstractIntegrationTest;
import com.yitaihuang.ticketrush.common.ApiException;
import com.yitaihuang.ticketrush.event.EventRepository;
import com.yitaihuang.ticketrush.reservation.StockGateway;

class OrderLifecycleIntegrationTest extends AbstractIntegrationTest {

	@Autowired
	OrderService orderService;

	@Autowired
	OrderExpiryJob expiryJob;

	@Autowired
	EventRepository events;

	@Autowired
	StockGateway stock;

	@Autowired
	JdbcTemplate jdbc;

	@Test
	void unpaidOrdersExpireAndTheirTicketsGoBackOnSale() {
		long eventId = createEvent(5, 4);
		String orderId = reserveAndAwaitOrder(eventId, "frank", 3);
		assertThat(stock.available(eventId)).hasValue(2);

		jdbc.update("UPDATE orders SET expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 MINUTE WHERE id = ?", orderId);
		expiryJob.run();

		assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.EXPIRED);
		assertThat(events.findById(eventId).orElseThrow().getAvailableTickets()).isEqualTo(5);
		assertThat(stock.available(eventId)).hasValue(5);
		assertThatThrownBy(() -> orderService.pay(orderId, "frank")).isInstanceOf(ApiException.class)
			.hasFieldOrPropertyWithValue("code", "INVALID_ORDER_STATE");
	}

	@Test
	void cancellingAnOrderReleasesItsTickets() {
		long eventId = createEvent(5, 4);
		String orderId = reserveAndAwaitOrder(eventId, "grace", 2);

		orderService.cancel(orderId, "grace");

		assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.CANCELLED);
		assertThat(events.findById(eventId).orElseThrow().getAvailableTickets()).isEqualTo(5);
		assertThat(stock.available(eventId)).hasValue(5);
	}

	@Test
	void redeliveredKafkaMessagesDoNotCreateDuplicateOrders() {
		long eventId = createEvent(5, 4);
		var request = new OrderRequested(UUID.randomUUID().toString(), eventId, "heidi", 1, 2500, "k", Instant.now());
		stock.reserve(eventId, "heidi", 1, 4, "k", request.orderId(), json.writeValueAsString(request), SALE_OPEN);

		orderService.recordRequest(request);
		orderService.recordRequest(request);

		assertThat(orders.countByEventIdAndStatus(eventId, OrderStatus.PENDING_PAYMENT)).isEqualTo(1);
		assertThat(events.findById(eventId).orElseThrow().getAvailableTickets()).isEqualTo(4);
	}

	@Test
	void mysqlStillPreventsOversellingWhenRedisIsOutOfSync() {
		long eventId = createEvent(1, 1);
		// Simulate Redis drifting above the real inventory, e.g. a bad manual fix.
		stock.initialize(eventId, 3);

		String first = reserveAndAwaitOrder(eventId, "ivan", 1);
		String second = reserveAndAwaitOrder(eventId, "judy", 1);

		assertThat(orders.findById(first).orElseThrow().getStatus()).isEqualTo(OrderStatus.PENDING_PAYMENT);
		assertThat(orders.findById(second).orElseThrow().getStatus()).isEqualTo(OrderStatus.REJECTED);
		assertThat(events.findById(eventId).orElseThrow().getAvailableTickets()).isZero();
	}

}
