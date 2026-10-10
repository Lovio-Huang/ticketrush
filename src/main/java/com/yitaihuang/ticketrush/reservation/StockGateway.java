package com.yitaihuang.ticketrush.reservation;

import java.time.Duration;
import java.util.List;
import java.util.OptionalInt;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import com.yitaihuang.ticketrush.config.TicketrushProperties;

/**
 * Redis-backed ticket inventory. All state changes go through Lua scripts so
 * each check-and-update is atomic; that is what prevents overselling under
 * thousands of concurrent requests without locking a database row.
 *
 * <p>Every key for an event shares the {@code {event:<id>}} hash tag, so the
 * scripts keep working if Redis is moved to a cluster.
 */
@Component
public class StockGateway {

	@SuppressWarnings("rawtypes")
	private static final RedisScript<List> RESERVE = RedisScript.of(new ClassPathResource("redis/reserve.lua"),
			List.class);

	private static final RedisScript<Long> RELEASE = RedisScript.of(new ClassPathResource("redis/release.lua"),
			Long.class);

	private final StringRedisTemplate redis;

	private final Duration idempotencyTtl;

	public StockGateway(StringRedisTemplate redis, TicketrushProperties properties) {
		this.redis = redis;
		this.idempotencyTtl = properties.reservations().idempotencyTtl();
	}

	public enum Outcome {

		RESERVED, DUPLICATE, SOLD_OUT, LIMIT_EXCEEDED, NOT_INITIALIZED

	}

	/** {@code orderId} is the new order on success, or the original order for a duplicate. */
	public record Reservation(Outcome outcome, String orderId) {
	}

	@SuppressWarnings("unchecked")
	public Reservation reserve(long eventId, String userId, int quantity, int perUserLimit, String idempotencyKey,
			String newOrderId) {
		List<String> result = redis.execute(RESERVE,
				List.of(stockKey(eventId), buyersKey(eventId), idempotencyKey(eventId, userId, idempotencyKey)),
				userId, String.valueOf(quantity), String.valueOf(perUserLimit), newOrderId,
				String.valueOf(idempotencyTtl.toSeconds()));
		return new Reservation(Outcome.valueOf(result.get(0)), result.get(1));
	}

	/** Puts tickets back after an order expired or was cancelled. */
	public void release(long eventId, String userId, int quantity) {
		redis.execute(RELEASE, List.of(stockKey(eventId), buyersKey(eventId)), userId, String.valueOf(quantity));
	}

	/**
	 * Undoes a reservation that never reached Kafka. Also forgets the idempotency
	 * key: the request effectively failed, so a retry should get a fresh attempt.
	 */
	public void releaseAndForget(long eventId, String userId, int quantity, String idempotencyKey) {
		redis.execute(RELEASE,
				List.of(stockKey(eventId), buyersKey(eventId), idempotencyKey(eventId, userId, idempotencyKey)),
				userId, String.valueOf(quantity));
	}

	/** Lowers a user's per-event count without touching stock (order rejected by MySQL). */
	public void releaseBuyerHold(long eventId, String userId, int quantity) {
		redis.opsForHash().increment(buyersKey(eventId), userId, -quantity);
	}

	public void initialize(long eventId, int available) {
		redis.opsForValue().set(stockKey(eventId), String.valueOf(available));
	}

	/** Seeds stock from MySQL after a Redis restart; a no-op if another instance won the race. */
	public void initializeIfAbsent(long eventId, int available) {
		redis.opsForValue().setIfAbsent(stockKey(eventId), String.valueOf(available));
	}

	public OptionalInt available(long eventId) {
		String value = redis.opsForValue().get(stockKey(eventId));
		return value == null ? OptionalInt.empty() : OptionalInt.of(Integer.parseInt(value));
	}

	static String stockKey(long eventId) {
		return "ticketrush:{event:" + eventId + "}:stock";
	}

	static String buyersKey(long eventId) {
		return "ticketrush:{event:" + eventId + "}:buyers";
	}

	static String idempotencyKey(long eventId, String userId, String key) {
		return "ticketrush:{event:" + eventId + "}:idem:" + userId + ":" + key;
	}

}
