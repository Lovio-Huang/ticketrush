package com.yitaihuang.ticketrush.order;

import java.time.Instant;

/** Kafka message: Redis granted the tickets, now MySQL needs a durable order. */
public record OrderRequested(String orderId, long eventId, String userId, int quantity, int amountCents,
		String idempotencyKey, Instant requestedAt) {

	/** The message that would have produced this order; used to rebuild Redis from MySQL. */
	public static OrderRequested of(Order order) {
		return new OrderRequested(order.getId(), order.getEventId(), order.getUserId(), order.getQuantity(),
				order.getAmountCents(), order.getIdempotencyKey(), order.getCreatedAt());
	}

}
