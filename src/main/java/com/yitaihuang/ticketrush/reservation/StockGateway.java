package com.yitaihuang.ticketrush.reservation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ScanOptions;
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

	private static final RedisScript<Long> CONFIRM = RedisScript.of(new ClassPathResource("redis/confirm.lua"),
			Long.class);

	private static final RedisScript<Long> SEED = RedisScript.of(new ClassPathResource("redis/seed.lua"), Long.class);

	@SuppressWarnings("rawtypes")
	private static final RedisScript<List> RECOUNT = RedisScript.of(new ClassPathResource("redis/recount.lua"),
			List.class);

	@SuppressWarnings("rawtypes")
	private static final RedisScript<List> DUE = RedisScript.of(new ClassPathResource("redis/due.lua"), List.class);

	/** Value of a released order in the orders hash. */
	static final String RELEASED = "R";

	private final StringRedisTemplate redis;

	private final Duration idempotencyTtl;

	private final Duration maxUnconfirmedAge;

	public StockGateway(StringRedisTemplate redis, TicketrushProperties properties) {
		this.redis = redis;
		this.idempotencyTtl = properties.reservations().idempotencyTtl();
		this.maxUnconfirmedAge = properties.reservations().maxUnconfirmedAge();
	}

	public enum Outcome {

		RESERVED, DUPLICATE, SOLD_OUT, LIMIT_EXCEEDED, NOT_INITIALIZED, QUEUE_BACKLOGGED

	}

	/** {@code orderId} is the new order on success, or the original order for a duplicate. */
	public record Reservation(Outcome outcome, String orderId) {
	}

	/**
	 * Reserves the tickets and records {@code orderJson} (the Kafka message) as a
	 * held order in the outbox, all in one atomic script.
	 */
	@SuppressWarnings("unchecked")
	public Reservation reserve(long eventId, String userId, int quantity, int perUserLimit, String idempotencyKey,
			String newOrderId, String orderJson) {
		List<String> result = redis.execute(RESERVE,
				List.of(stockKey(eventId), buyersKey(eventId), idempotencyKey(eventId, userId, idempotencyKey),
						ordersKey(eventId), outboxKey(eventId)),
				userId, String.valueOf(quantity), String.valueOf(perUserLimit), newOrderId,
				String.valueOf(idempotencyTtl.toSeconds()), orderJson, String.valueOf(maxUnconfirmedAge.toMillis()));
		return new Reservation(Outcome.valueOf(result.get(0)), result.get(1));
	}

	/**
	 * Puts an order's tickets back (expired, cancelled, rejected by MySQL).
	 * Idempotent per order: returns false if it was already released.
	 */
	public boolean release(long eventId, String orderId) {
		return Long.valueOf(1).equals(redis.execute(RELEASE, eventKeys(eventId), orderId));
	}

	/**
	 * Undoes a reservation that definitely never reached Kafka. Also forgets the
	 * idempotency key: the request failed, so a retry should get a fresh attempt.
	 */
	public void releaseAndForget(long eventId, String orderId, String userId, String idempotencyKey) {
		List<String> keys = new ArrayList<>(eventKeys(eventId));
		keys.add(idempotencyKey(eventId, userId, idempotencyKey));
		redis.execute(RELEASE, keys, orderId);
	}

	/** Kafka has stored the order: it no longer needs the outbox. */
	public void markPublished(long eventId, String orderId) {
		redis.opsForZSet().remove(outboxKey(eventId), orderId);
	}

	/**
	 * MySQL holds the order as active. Takes it out of the outbox and, if Redis
	 * had lost it, holds its tickets again. Returns whether it had to re-hold.
	 */
	public boolean confirm(long eventId, String orderId, String userId, String idempotencyKey, String orderJson,
			Duration idempotencyLeft) {
		List<String> keys = new ArrayList<>(eventKeys(eventId));
		keys.add(idempotencyKey(eventId, userId, idempotencyKey));
		return Long.valueOf(1).equals(redis.execute(CONFIRM, keys, orderId, orderJson,
				String.valueOf(Math.max(0, idempotencyLeft.toSeconds()))));
	}

	/** One order to put back during a rebuild: its JSON if active, {@code null} if it no longer holds tickets. */
	public record SeedOrder(String orderId, String userId, String idempotencyKey, String orderJson,
			Duration idempotencyLeft) {
	}

	public void seed(long eventId, List<SeedOrder> batch) {
		if (batch.isEmpty()) {
			return;
		}
		List<String> keys = new ArrayList<>(batch.size() + 1);
		List<String> args = new ArrayList<>(batch.size() * 3);
		keys.add(ordersKey(eventId));
		for (SeedOrder order : batch) {
			keys.add(idempotencyKey(eventId, order.userId(), order.idempotencyKey()));
			args.add(order.orderId());
			args.add(order.orderJson() == null ? RELEASED : order.orderJson());
			args.add(String.valueOf(Math.max(0, order.idempotencyLeft().toSeconds())));
		}
		redis.execute(SEED, keys, args.toArray());
	}

	/** Result of {@link #recount}: stock before ({@code -1} if missing) and after, and whether per-user counts moved. */
	public record Recount(long previous, long stock, boolean buyersChanged) {

		public boolean changed() {
			return previous != stock || buyersChanged;
		}

	}

	/** Atomically sets stock and per-user counts from the held orders (see recount.lua). */
	@SuppressWarnings("unchecked")
	public Recount recount(long eventId, int totalTickets) {
		List<Long> result = redis.execute(RECOUNT,
				List.of(stockKey(eventId), buyersKey(eventId), ordersKey(eventId)), String.valueOf(totalTickets));
		return new Recount(result.get(0), result.get(1), result.get(2) == 1L);
	}

	/** Order ids currently holding tickets, by scanning the orders hash. */
	public List<String> heldOrderIds(long eventId) {
		List<String> held = new ArrayList<>();
		HashOperations<String, String, String> hash = redis.opsForHash();
		try (Cursor<Map.Entry<String, String>> cursor = hash.scan(ordersKey(eventId),
				ScanOptions.scanOptions().count(500).build())) {
			cursor.forEachRemaining(entry -> {
				if (!RELEASED.equals(entry.getValue())) {
					held.add(entry.getKey());
				}
			});
		}
		return held;
	}

	/** Orders that have waited in the outbox longer than {@code minAge}, as orderId -> order JSON. */
	@SuppressWarnings("unchecked")
	public Map<String, String> dueForRepublish(long eventId, Duration minAge, int limit) {
		List<String> flat = redis.execute(DUE, List.of(outboxKey(eventId), ordersKey(eventId)),
				String.valueOf(minAge.toMillis()), String.valueOf(limit));
		Map<String, String> due = new LinkedHashMap<>();
		for (int i = 0; i + 1 < flat.size(); i += 2) {
			due.put(flat.get(i), flat.get(i + 1));
		}
		return due;
	}

	public long outboxSize(long eventId) {
		Long size = redis.opsForZSet().size(outboxKey(eventId));
		return size == null ? 0 : size;
	}

	/**
	 * Lets one instance rebuild an event's Redis state; others back off until the
	 * lease expires. shortcut: a plain SET NX lease, not a fenced lock; fine because
	 * every rebuild step is idempotent and the last step is one atomic recount.
	 */
	public boolean tryLeaseRebuild(long eventId, Duration lease) {
		return Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(rebuildKey(eventId), "1", lease));
	}

	public void shortenRebuildLease(long eventId, Duration lease) {
		redis.expire(rebuildKey(eventId), lease);
	}

	public void endRebuild(long eventId) {
		redis.delete(rebuildKey(eventId));
	}

	public void initialize(long eventId, int available) {
		redis.opsForValue().set(stockKey(eventId), String.valueOf(available));
	}

	public OptionalInt available(long eventId) {
		String value = redis.opsForValue().get(stockKey(eventId));
		return value == null ? OptionalInt.empty() : OptionalInt.of(Integer.parseInt(value));
	}

	/** Stock for several events in one MGET; events without a Redis key are left out. */
	public Map<Long, Integer> availableMany(List<Long> eventIds) {
		Map<Long, Integer> result = new HashMap<>();
		if (eventIds.isEmpty()) {
			return result;
		}
		List<String> values = redis.opsForValue().multiGet(eventIds.stream().map(StockGateway::stockKey).toList());
		for (int i = 0; i < eventIds.size(); i++) {
			if (values != null && values.get(i) != null) {
				result.put(eventIds.get(i), Integer.parseInt(values.get(i)));
			}
		}
		return result;
	}

	private static List<String> eventKeys(long eventId) {
		return List.of(stockKey(eventId), buyersKey(eventId), ordersKey(eventId), outboxKey(eventId));
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

	static String ordersKey(long eventId) {
		return "ticketrush:{event:" + eventId + "}:orders";
	}

	static String outboxKey(long eventId) {
		return "ticketrush:{event:" + eventId + "}:outbox";
	}

	static String rebuildKey(long eventId) {
		return "ticketrush:{event:" + eventId + "}:rebuild";
	}

}
