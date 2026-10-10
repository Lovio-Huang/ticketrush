package com.yitaihuang.ticketrush.order;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.yitaihuang.ticketrush.common.ApiException;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

	private final OrderService orders;

	private final PendingOrderTracker pendingOrders;

	public OrderController(OrderService orders, PendingOrderTracker pendingOrders) {
		this.orders = orders;
		this.pendingOrders = pendingOrders;
	}

	@JsonInclude(JsonInclude.Include.NON_NULL)
	public record OrderResponse(String orderId, String status, Long eventId, Integer quantity, Integer amountCents,
			Instant expiresAt, Instant createdAt) {

		static OrderResponse of(Order order) {
			return new OrderResponse(order.getId(), order.getStatus().name(), order.getEventId(), order.getQuantity(),
					order.getAmountCents(), order.getExpiresAt(), order.getCreatedAt());
		}

		static OrderResponse processing(String orderId) {
			return new OrderResponse(orderId, "PROCESSING", null, null, null, null, null);
		}

	}

	@GetMapping("/{orderId}")
	public OrderResponse get(@PathVariable String orderId, @RequestHeader("X-User-Id") String userId) {
		return orders.find(orderId, userId).map(OrderResponse::of).orElseGet(() -> {
			if (pendingOrders.isProcessingFor(orderId, userId)) {
				return OrderResponse.processing(orderId);
			}
			throw ApiException.notFound("ORDER_NOT_FOUND", "Order " + orderId + " does not exist");
		});
	}

	@PostMapping("/{orderId}/payment")
	public OrderResponse pay(@PathVariable String orderId, @RequestHeader("X-User-Id") String userId) {
		return OrderResponse.of(orders.pay(orderId, userId));
	}

	@PostMapping("/{orderId}/cancellation")
	public OrderResponse cancel(@PathVariable String orderId, @RequestHeader("X-User-Id") String userId) {
		return OrderResponse.of(orders.cancel(orderId, userId));
	}

}
