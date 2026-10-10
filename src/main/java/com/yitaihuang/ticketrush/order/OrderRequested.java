package com.yitaihuang.ticketrush.order;

import java.time.Instant;

/** Kafka message: Redis granted the tickets, now MySQL needs a durable order. */
public record OrderRequested(String orderId, long eventId, String userId, int quantity, int amountCents,
		String idempotencyKey, Instant requestedAt) {
}
