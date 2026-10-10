package com.yitaihuang.ticketrush.order;

import java.time.Duration;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Remembers orders that were accepted but not yet written by the consumer, so
 * polling clients see "PROCESSING" instead of a confusing 404 for that gap.
 */
@Component
public class PendingOrderTracker {

	private static final Duration TTL = Duration.ofMinutes(10);

	private final StringRedisTemplate redis;

	public PendingOrderTracker(StringRedisTemplate redis) {
		this.redis = redis;
	}

	public void markProcessing(String orderId, String userId) {
		redis.opsForValue().set(key(orderId), userId, TTL);
	}

	boolean isProcessingFor(String orderId, String userId) {
		return userId.equals(redis.opsForValue().get(key(orderId)));
	}

	private static String key(String orderId) {
		return "ticketrush:order:" + orderId + ":processing";
	}

}
