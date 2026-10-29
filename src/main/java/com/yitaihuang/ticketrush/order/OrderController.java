package com.yitaihuang.ticketrush.order;

import java.time.Instant;
import java.util.Optional;

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

	private final OrderFailureService failures;

	public OrderController(OrderService orders, PendingOrderTracker pendingOrders, OrderFailureService failures) {
		this.orders = orders;
		this.pendingOrders = pendingOrders;
		this.failures = failures;
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

		/** What the customer asked for, without the failure's internals (error, payload, operator notes). */
		static OrderResponse of(OrderRequested request, String status) {
			return new OrderResponse(request.orderId(), status, request.eventId(), request.quantity(),
					request.amountCents(), null, null);
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
		return orders.find(orderId, userId)
			.map(OrderResponse::of)
			.or(() -> failures.findForCustomer(orderId, userId).flatMap(OrderController::shownAs))
			.orElseGet(() -> {
				if (pendingOrders.isProcessingFor(orderId, userId)) {
					return OrderResponse.processing(orderId);
				}
				throw ApiException.notFound("ORDER_NOT_FOUND", "Order " + orderId + " does not exist");
			});
	}

	/**
	 * A dead-lettered order that isn't in the orders table (yet): accepted, its
	 * tickets held, waiting for an operator (UNDER_REVIEW); sent again
	 * (PROCESSING, until the stored order takes over); or given up (REJECTED).
	 * A discarded one always has its order stored, which is shown instead.
	 */
	private static Optional<OrderResponse> shownAs(OrderFailureService.CustomerView failure) {
		return switch (failure.status()) {
			case OPEN -> Optional.of(OrderResponse.of(failure.request(), "UNDER_REVIEW"));
			case REPLAYED -> Optional.of(OrderResponse.of(failure.request(), "PROCESSING"));
			case VOIDED -> Optional.of(OrderResponse.of(failure.request(), "REJECTED"));
			case DISCARDED -> Optional.empty();
		};
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
