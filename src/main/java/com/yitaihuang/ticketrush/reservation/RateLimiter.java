package com.yitaihuang.ticketrush.reservation;

import java.util.List;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import com.yitaihuang.ticketrush.config.TicketrushProperties;

/**
 * Per-user token bucket stored in Redis, so the limit holds across all app
 * instances instead of resetting per JVM.
 */
@Component
public class RateLimiter {

	private static final RedisScript<Long> TOKEN_BUCKET = RedisScript
		.of(new ClassPathResource("redis/rate_limit.lua"), Long.class);

	private final StringRedisTemplate redis;

	private final String capacity;

	private final String refillPerSecond;

	public RateLimiter(StringRedisTemplate redis, TicketrushProperties properties) {
		this.redis = redis;
		this.capacity = String.valueOf(properties.rateLimit().capacity());
		this.refillPerSecond = String.valueOf(properties.rateLimit().refillPerSecond());
	}

	public boolean tryAcquire(String userId) {
		Long allowed = redis.execute(TOKEN_BUCKET, List.of("ticketrush:ratelimit:" + userId), capacity,
				refillPerSecond);
		return allowed != null && allowed == 1L;
	}

}
