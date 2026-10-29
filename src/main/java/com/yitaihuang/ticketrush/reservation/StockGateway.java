package com.yitaihuang.ticketrush.reservation;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisCallback;
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
 * <p>Every key for an event shares the {@code {event:<id>}} hash tag, so each
 * per-event script touches one slot. Only a single Redis server is supported:
 * the restart check and its wipe span every event and the global server keys,
 * which a Redis Cluster would refuse as cross-slot.
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

	private static final RedisScript<Long> RESTART_CLAIM = RedisScript
		.of(new ClassPathResource("redis/restart_claim.lua"), Long.class);

	private static final RedisScript<Long> WIPE_BATCH = RedisScript.of(new ClassPathResource("redis/wipe_batch.lua"),
			Long.class);

	private static final RedisScript<Long> RESTART_TRUST = RedisScript
		.of(new ClassPathResource("redis/restart_trust.lua"), Long.class);

	private static final RedisScript<Long> SEED = RedisScript.of(new ClassPathResource("redis/seed.lua"), Long.class);

	@SuppressWarnings("rawtypes")
	private static final RedisScript<List> RECOUNT = RedisScript.of(new ClassPathResource("redis/recount.lua"),
			List.class);

	@SuppressWarnings("rawtypes")
	private static final RedisScript<List> DUE = RedisScript.of(new ClassPathResource("redis/due.lua"), List.class);

	@SuppressWarnings("rawtypes")
	private static final RedisScript<List> REBUILD_BEGIN = RedisScript
		.of(new ClassPathResource("redis/rebuild_begin.lua"), List.class);

	private static final RedisScript<Long> DRIFT = RedisScript.of(new ClassPathResource("redis/drift.lua"),
			Long.class);

	private static final RedisScript<Long> REBUILD_LEASE = RedisScript
		.of(new ClassPathResource("redis/rebuild_lease.lua"), Long.class);

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

		RESERVED, DUPLICATE, SOLD_OUT, LIMIT_EXCEEDED, NOT_INITIALIZED, QUEUE_BACKLOGGED, SALE_ENDED

	}

	/** {@code orderId} is the new order on success, or the original order for a duplicate. */
	public record Reservation(Outcome outcome, String orderId) {
	}

	/**
	 * Reserves the tickets and records {@code orderJson} (the Kafka message) as a
	 * held order in the outbox, all in one atomic script. A new reservation at or
	 * after {@code saleEndsAt} by Redis's clock is refused ({@link Outcome#SALE_ENDED});
	 * a duplicate of one accepted before still returns its order.
	 */
	@SuppressWarnings("unchecked")
	public Reservation reserve(long eventId, String userId, int quantity, int perUserLimit, String idempotencyKey,
			String newOrderId, String orderJson, Instant saleEndsAt) {
		List<String> result = redis.execute(RESERVE,
				List.of(stockKey(eventId), buyersKey(eventId), idempotencyKey(eventId, userId, idempotencyKey),
						ordersKey(eventId), outboxKey(eventId), heldKey(eventId)),
				userId, String.valueOf(quantity), String.valueOf(perUserLimit), newOrderId,
				String.valueOf(idempotencyTtl.toSeconds()), orderJson, String.valueOf(maxUnconfirmedAge.toMillis()),
				String.valueOf(saleEndsAt.toEpochMilli()));
		return new Reservation(Outcome.valueOf(result.get(0)), result.get(1));
	}

	/** The order a request with this idempotency key was accepted as, while Redis still remembers it. */
	public Optional<String> acceptedOrderId(long eventId, String userId, String idempotencyKey) {
		return Optional.ofNullable(redis.opsForValue().get(idempotencyKey(eventId, userId, idempotencyKey)));
	}

	/**
	 * Puts an order's tickets back (expired, cancelled, rejected by MySQL, voided).
	 * Idempotent per order: returns false if it was already released.
	 */
	public boolean release(long eventId, String orderId) {
		return Long.valueOf(1).equals(redis.execute(RELEASE, eventKeys(eventId), orderId, ""));
	}

	/**
	 * Undoes a reservation whose own send failed before the record was queued, and
	 * forgets its idempotency key so a retry gets a fresh attempt. Refused (false)
	 * if the relay or the consumer has already taken the order: then a copy may be
	 * in Kafka and the order must stay held.
	 */
	public boolean releaseAndForget(long eventId, String orderId, String userId, String idempotencyKey) {
		List<String> keys = new ArrayList<>(eventKeys(eventId));
		keys.add(idempotencyKey(eventId, userId, idempotencyKey));
		return !Long.valueOf(-1).equals(redis.execute(RELEASE, keys, orderId, "not-sent"));
	}

	/** Kafka has stored the order: it no longer needs the outbox. */
	public void markPublished(long eventId, String orderId) {
		redis.opsForZSet().remove(outboxKey(eventId), orderId);
	}

	public enum Claim {

		/** Redis holds the order; it is now marked sent and out of the outbox. */
		HELD,

		/** Redis had lost the order (a rebuild is running, or MySQL has it) and holds it again now. */
		REHELD,

		/** Redis released the order, or was rebuilt without it: it must not get tickets. */
		RELEASED

	}

	/**
	 * The consumer's claim on an order, taken before it writes the order to MySQL
	 * (see confirm.lua). Atomic with releases: an order is claimed or released,
	 * never both. {@code firstDelivery} is true when MySQL doesn't have the order:
	 * then an order Redis doesn't know while the event's state exists was left out
	 * of a rebuild (it arrived after its barrier), and is released instead.
	 */
	public Claim confirm(long eventId, String orderId, String userId, String idempotencyKey, String orderJson,
			Duration idempotencyLeft, boolean firstDelivery) {
		List<String> keys = new ArrayList<>(eventKeys(eventId));
		keys.add(idempotencyKey(eventId, userId, idempotencyKey));
		Long result = redis.execute(CONFIRM, keys, orderId, orderJson,
				String.valueOf(Math.max(0, idempotencyLeft.toSeconds())), firstDelivery ? "new" : "");
		return result == -1L ? Claim.RELEASED : result == 1L ? Claim.REHELD : Claim.HELD;
	}

	/** One order to put back during a rebuild: its JSON if active, {@code null} if it no longer holds tickets. */
	public record SeedOrder(String orderId, String userId, String idempotencyKey, String orderJson,
			Duration idempotencyLeft) {
	}

	public enum RebuildStart {

		/** The stock key exists again: nothing to rebuild. */
		DONE,

		/** Another rebuilder holds the lease. */
		BUSY,

		/** The quiet period after the loss hasn't passed: sends from before it may still arrive. */
		WAIT,

		/** This caller holds the lease. */
		GO

	}

	/** Asks for the right to rebuild (see rebuild_begin.lua). */
	@SuppressWarnings("unchecked")
	public RebuildStart beginRebuild(long eventId, String token, Duration lease, Duration quietPeriod) {
		List<Object> result = redis.execute(REBUILD_BEGIN,
				List.of(stockKey(eventId), rebuildKey(eventId), recoveringSinceKey(eventId)), token,
				String.valueOf(lease.toMillis()), String.valueOf(quietPeriod.toMillis()));
		return RebuildStart.valueOf((String) result.get(0));
	}

	/** Shortens this caller's lease ({@code Duration.ZERO} drops it); a lease someone else holds is left alone. */
	public void releaseRebuildLease(long eventId, String token, Duration keepFor) {
		redis.execute(REBUILD_LEASE, List.of(rebuildKey(eventId)), token, String.valueOf(keepFor.toMillis()));
	}

	/** Seeds one batch under this caller's lease; false if the lease is no longer theirs. */
	public boolean seed(long eventId, String token, Duration lease, List<SeedOrder> batch) {
		List<String> keys = new ArrayList<>(batch.size() + 2);
		List<String> args = new ArrayList<>(batch.size() * 3 + 2);
		keys.add(rebuildKey(eventId));
		keys.add(ordersKey(eventId));
		args.add(token);
		args.add(String.valueOf(lease.toMillis()));
		for (SeedOrder order : batch) {
			keys.add(idempotencyKey(eventId, order.userId(), order.idempotencyKey()));
			args.add(order.orderId());
			args.add(order.orderJson() == null ? RELEASED : order.orderJson());
			args.add(String.valueOf(Math.max(0, order.idempotencyLeft().toSeconds())));
		}
		Long seeded = redis.execute(SEED, keys, args.toArray());
		return seeded != null && seeded >= 0;
	}

	/** Result of {@link #recount}: stock before ({@code -1} if missing) and after, and whether per-user counts moved. */
	public record Recount(long previous, long stock, boolean buyersChanged) {

		public boolean changed() {
			return previous != stock || buyersChanged;
		}

	}

	/**
	 * Atomically resets stock and per-user counts from the held orders on a live
	 * event (see recount.lua). Does nothing while the stock key is missing.
	 */
	public Recount recount(long eventId, int totalTickets) {
		return runRecount(eventId, totalTickets, "");
	}

	/** The last step of a rebuild: recounts and creates the stock key, if the lease is still this caller's. */
	public Optional<Recount> finishRebuild(long eventId, String token, int totalTickets) {
		Recount recount = runRecount(eventId, totalTickets, token);
		return recount.previous() == -2 ? Optional.empty() : Optional.of(recount);
	}

	@SuppressWarnings("unchecked")
	private Recount runRecount(long eventId, int totalTickets, String token) {
		List<Long> result = redis.execute(RECOUNT, List.of(stockKey(eventId), buyersKey(eventId), ordersKey(eventId),
				rebuildKey(eventId), recoveringSinceKey(eventId), heldKey(eventId)), String.valueOf(totalTickets), token);
		return new Recount(result.get(0), result.get(1), result.get(2) == 1L);
	}

	/** O(1): whether stock + held tickets no longer add up to the total, so a full recount is due (drift.lua). */
	public boolean drifted(long eventId, int totalTickets) {
		return Long.valueOf(1).equals(
				redis.execute(DRIFT, List.of(stockKey(eventId), heldKey(eventId)), String.valueOf(totalTickets)));
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
		List<String> flat = redis.execute(DUE, List.of(outboxKey(eventId), ordersKey(eventId), sentKey(eventId)),
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

	/** The Redis server's run_id: a new one after every restart, and on a different server after failover. */
	public String serverRunId() {
		Properties info = redis.execute((RedisCallback<Properties>) connection -> connection.serverCommands().info("server"));
		return info.getProperty("run_id");
	}

	public enum RestartCheck {

		/** The current server's state is trusted. */
		TRUSTED,

		/** The server changed and this caller holds the wipe lock: it must wipe, then {@link #trustServer}. */
		WIPE,

		/** The server changed and another caller is wiping. */
		BUSY

	}

	/**
	 * Compares the server with the trusted one and, if they differ, takes the wipe
	 * lock for {@code owner}, atomically (restart_claim.lua). The owner token must
	 * be new for every attempt: it is what tells a wiper apart from one that lost
	 * its lease, since both see the same run_id.
	 */
	public RestartCheck checkServer(String server, Duration wipeLease, String owner) {
		Long result = redis.execute(RESTART_CLAIM, List.of(RedisRestartGuard.SERVER_KEY, RedisRestartGuard.WIPE_LOCK),
				server, String.valueOf(wipeLease.toMillis()), owner);
		return RestartCheck.values()[result.intValue()];
	}

	/** Trusts the server and frees the wipe lock, only if {@code owner} still holds it (restart_trust.lua). */
	public boolean trustServer(String server, String owner) {
		Long trusted = redis.execute(RESTART_TRUST, List.of(RedisRestartGuard.SERVER_KEY, RedisRestartGuard.WIPE_LOCK),
				server, owner);
		return trusted != null && trusted == 1;
	}

	/**
	 * Deletes every event's Redis state, for when Redis can't be trusted to have
	 * all of it (see {@link RedisRestartGuard}). Stock keys go first, so each
	 * event stops taking reservations at once and is then rebuilt from MySQL.
	 * Each batch is deleted only while {@code owner} holds the wipe lock, and
	 * renews it; empty once the lock is lost, with nothing more deleted.
	 */
	public OptionalInt wipeAllEvents(String owner, Duration lease) {
		int stocks = unlinkMatching("ticketrush:{event:*}:stock", owner, lease);
		int rest = stocks < 0 ? -1 : unlinkMatching("ticketrush:{event:*", owner, lease);
		return rest < 0 ? OptionalInt.empty() : OptionalInt.of(stocks + rest);
	}

	/** The keys deleted, or -1 as soon as {@code owner} no longer holds the wipe lock. */
	private int unlinkMatching(String pattern, String owner, Duration lease) {
		List<String> batch = new ArrayList<>();
		int deleted = 0;
		try (Cursor<String> keys = redis.scan(ScanOptions.scanOptions().match(pattern).count(1000).build())) {
			while (keys.hasNext()) {
				batch.add(keys.next());
				if (batch.size() == 500) {
					if (!unlinkOwned(batch, owner, lease)) {
						return -1;
					}
					deleted += batch.size();
					batch.clear();
				}
			}
		}
		// Also run when the batch is empty: it confirms the lock is still ours.
		if (!unlinkOwned(batch, owner, lease)) {
			return -1;
		}
		return deleted + batch.size();
	}

	/** One atomic step (wipe_batch.lua): check that {@code owner} holds the wipe lock, renew it, delete the keys. */
	boolean unlinkOwned(List<String> batch, String owner, Duration lease) {
		List<String> keys = new ArrayList<>(batch.size() + 1);
		keys.add(RedisRestartGuard.WIPE_LOCK);
		keys.addAll(batch);
		Long result = redis.execute(WIPE_BATCH, keys, owner, String.valueOf(lease.toMillis()));
		return result != null && result >= 0;
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
		return List.of(stockKey(eventId), buyersKey(eventId), ordersKey(eventId), outboxKey(eventId),
				sentKey(eventId), heldKey(eventId));
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

	static String heldKey(long eventId) {
		return "ticketrush:{event:" + eventId + "}:held";
	}

	static String sentKey(long eventId) {
		return "ticketrush:{event:" + eventId + "}:sent";
	}

	static String rebuildKey(long eventId) {
		return "ticketrush:{event:" + eventId + "}:rebuild";
	}

	static String recoveringSinceKey(long eventId) {
		return "ticketrush:{event:" + eventId + "}:recovering-since";
	}

}
