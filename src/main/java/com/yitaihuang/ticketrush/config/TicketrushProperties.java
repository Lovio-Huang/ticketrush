package com.yitaihuang.ticketrush.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("ticketrush")
public record TicketrushProperties(Orders orders, Reservations reservations, RateLimit rateLimit, Security security,
		Reconciliation reconciliation) {

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
	 * @param republishAfter how long an accepted order may wait for a Kafka ack before the relay sends it again
	 * @param maxUnconfirmedAge if an event's oldest unconfirmed order is older than this, new reservations for it
	 * are refused until Kafka catches up
	 * @param relayInterval how often the relay looks for unconfirmed orders
	 */
	public record Reservations(Duration idempotencyTtl, Duration publishTimeout,
			@DefaultValue("PT10S") Duration republishAfter, @DefaultValue("PT30S") Duration maxUnconfirmedAge,
			@DefaultValue("PT2S") Duration relayInterval) {
	}

	/**
	 * @param capacity max burst of reservation requests per user
	 * @param refillPerSecond sustained requests per second per user
	 */
	public record RateLimit(int capacity, double refillPerSecond) {
	}

	/**
	 * @param jwtSecret base64-encoded HS256 key, at least 32 bytes
	 * @param tokenTtl lifetime of an access token
	 * @param adminUsername admin account created at startup if it doesn't exist (optional)
	 * @param adminPassword password for that admin account (optional)
	 * @param loginAttemptsPerMinute login attempts allowed per account per minute (password guessing)
	 * @param authRequestsPerMinutePerAddress logins and sign-ups allowed per client address per minute
	 */
	public record Security(String jwtSecret, Duration tokenTtl, String adminUsername, String adminPassword,
			@DefaultValue("5") int loginAttemptsPerMinute, @DefaultValue("30") int authRequestsPerMinutePerAddress) {
	}

	/**
	 * @param interval how often Redis is checked against MySQL
	 */
	public record Reconciliation(Duration interval) {
	}

}
