package com.yitaihuang.ticketrush.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("ticketrush")
public record TicketrushProperties(Orders orders, Reservations reservations, RateLimit rateLimit) {

	/**
	 * @param topic Kafka topic that carries accepted reservations to the order writer
	 * @param partitions partitions for that topic; orders are keyed by event id
	 * @param paymentWindow how long a reserved order stays payable before it expires
	 * @param expiryScanInterval how often the expiry job looks for unpaid orders
	 * @param expiryBatchSize max orders expired per database transaction
	 */
	public record Orders(String topic, int partitions, Duration paymentWindow, Duration expiryScanInterval,
			int expiryBatchSize) {
	}

	/**
	 * @param idempotencyTtl how long a client's Idempotency-Key is remembered
	 * @param publishTimeout how long to wait for Kafka to acknowledge an order
	 */
	public record Reservations(Duration idempotencyTtl, Duration publishTimeout) {
	}

	/**
	 * @param capacity max burst of reservation requests per user
	 * @param refillPerSecond sustained requests per second per user
	 */
	public record RateLimit(int capacity, double refillPerSecond) {
	}

}
