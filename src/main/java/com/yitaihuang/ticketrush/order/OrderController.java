package com.yitaihuang.ticketrush.order;

import java.time.Instant;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.springframework.data.domain.PageRequest;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.yitaihuang.ticketrush.common.ApiException;
import com.yitaihuang.ticketrush.common.PageResponse;

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

	/** The caller's orders, newest first, optionally filtered by status. */
	@GetMapping
	public PageResponse<OrderResponse> mine(@AuthenticationPrincipal Jwt jwt,
			@RequestParam(required = false) OrderStatus status, @RequestParam(defaultValue = "0") @Min(0) int page,
			@RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
		return PageResponse.of(orders.history(jwt.getSubject(), status, PageRequest.of(page, size)),
				OrderResponse::of);
	}

	@GetMapping("/{orderId}")
	public OrderResponse get(@PathVariable String orderId, @AuthenticationPrincipal Jwt jwt) {
		String userId = jwt.getSubject();
		return orders.find(orderId, userId).map(OrderResponse::of).orElseGet(() -> {
			if (pendingOrders.isProcessingFor(orderId, userId)) {
				return OrderResponse.processing(orderId);
			}
			throw ApiException.notFound("ORDER_NOT_FOUND", "Order " + orderId + " does not exist");
		});
	}

	@PostMapping("/{orderId}/payment")
	public OrderResponse pay(@PathVariable String orderId, @AuthenticationPrincipal Jwt jwt) {
		return OrderResponse.of(orders.pay(orderId, jwt.getSubject()));
	}

	@PostMapping("/{orderId}/cancellation")
	public OrderResponse cancel(@PathVariable String orderId, @AuthenticationPrincipal Jwt jwt) {
		return OrderResponse.of(orders.cancel(orderId, jwt.getSubject()));
	}

}
