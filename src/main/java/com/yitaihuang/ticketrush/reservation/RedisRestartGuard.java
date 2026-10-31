package com.yitaihuang.ticketrush.reservation;

import java.time.Duration;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Turns every Redis restart or failover into a full loss, the one Redis failure
 * the recovery is built for.
 *
 * <p>A Redis that comes back from an AOF or RDB file, or a replica promoted after
 * the primary died, holds an older copy of the events' state: the orders,
 * tickets and idempotency keys of its last second (or hours) are missing, but
 * the stock key is there, so nothing else would notice and it would keep selling
 * on that state. No timestamp tells such an order apart from one whose request
 * paused before its send, so instead every second this compares the server's
 * run_id (new after each restart, different on another server) with the one
 * recorded in Redis itself. On a mismatch exactly one instance (the compare and
 * the wipe lock are one script, restart_claim.lua) deletes every event's keys,
 * stock first, and the fenced rebuild recreates them from MySQL. The lock holds
 * a token new to each attempt, and every delete batch, lease renewal and the
 * final "trusted" mark checks it in the same script: a wiper that stalled past
 * its lease and was taken over stops, without deleting what its successor has
 * rebuilt since, freeing its lock, or reporting the wipe done. A missing
 * record (a fresh or emptied Redis) counts as a mismatch. The record travels
 * with the data, so a snapshot restored while every app was down carries the
 * old server's id and is caught by the first check.
 *
 * <p>A process refuses reservations and rebuilds ({@link #verified}) unless a
 * check that passed started within {@code redis-check-max-age} (default: two
 * check intervals, 2 s), measured on a monotonic clock. So it never trusts state
 * before checking it, nor while another instance is still wiping, nor once its
 * own checks have stopped coming: a check stuck on Redis, a paused process, a
 * starved thread. The checks run on a thread of their own, every
 * {@code redis-check-interval} (1 s), so other jobs can't delay them.
 *
 * <p>What stays open: a running instance trusts its last passing check for up to
 * the max age, so for that long after its Redis is replaced by one with older
 * data it may still sell on that state. That is a configured bound on how stale a
 * verdict may be, not a hard real-time guarantee on when the next check runs.
 * Compose runs Redis without persistence, so a restart there comes back empty
 * and is refused at once.
 */
@Component
public class RedisRestartGuard implements DisposableBean {

	private static final Logger log = LoggerFactory.getLogger(RedisRestartGuard.class);

	/** The run_id of the server whose state the app last trusted. */
	public static final String SERVER_KEY = "ticketrush:redis-server";

	public static final String WIPE_LOCK = "ticketrush:redis-server:wiping";

	/** Long enough to wipe every event; if the wiper dies, the next check after it redoes the wipe. */
	private static final Duration WIPE_LEASE = Duration.ofMinutes(1);

	private final StockGateway stock;

	private final Counter detected;

	private final Duration interval;

	private final long maxAgeNanos;

	/** Monotonic nanoseconds (System::nanoTime); a test can drive it. */
	private final LongSupplier nanoTime;

	/**
	 * Default deny: false until the first check, and whenever the last one found a
	 * changed server or failed. Reservations and rebuilds wait for it, so a process
	 * that just started on top of a restored snapshot, or that runs while another
	 * instance is (re)doing a wipe, can't sell on, or rebuild into, state that isn't
	 * trusted. It gates this process only; the wipe itself is decided once, in Redis.
	 */
	private volatile boolean passed;

	/** When the latest check that passed started; only meaningful while {@link #passed}. */
	private volatile long passedAtNanos;

	private ScheduledExecutorService checks;

	@Autowired
	public RedisRestartGuard(StockGateway stock, MeterRegistry meters,
			@Value("${ticketrush.reservations.redis-check-interval:PT1S}") Duration interval,
			@Value("${ticketrush.reservations.redis-check-max-age:#{null}}") Duration maxAge) {
		this(stock, meters, interval, maxAge == null ? interval.multipliedBy(2) : maxAge, System::nanoTime);
	}

	/** A guard with the default timing, for tests that drive its checks themselves. */
	RedisRestartGuard(StockGateway stock, MeterRegistry meters) {
		this(stock, meters, Duration.ofSeconds(1), Duration.ofSeconds(2), System::nanoTime);
	}

	RedisRestartGuard(StockGateway stock, MeterRegistry meters, Duration interval, Duration maxAge,
			LongSupplier nanoTime) {
		this.stock = stock;
		this.detected = meters.counter("ticketrush.redis.restarts_detected");
		this.interval = interval;
		this.maxAgeNanos = maxAge.toNanos();
		this.nanoTime = nanoTime;
	}

	/** Checks once when the app is ready, then every interval on a thread of its own. */
	@EventListener(ApplicationReadyEvent.class)
	public void start() {
		run();
		checks = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().name("redis-restart-guard").factory());
		checks.scheduleWithFixedDelay(this::run, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public void destroy() {
		if (checks != null) {
			checks.shutdownNow();
		}
	}

	public void run() {
		try {
			check();
		}
		catch (RuntimeException ex) {
			passed = false;
			log.warn("Could not check the Redis server: {}", ex.toString());
		}
	}

	/** True only while this process's latest check passed and started no longer than the max age ago. */
	public boolean verified() {
		return passed && nanoTime.getAsLong() - passedAtNanos <= maxAgeNanos;
	}

	public void check() {
		long started = nanoTime.getAsLong();
		String server = stock.serverRunId();
		String owner = UUID.randomUUID().toString();
		switch (stock.checkServer(server, WIPE_LEASE, owner)) {
			case TRUSTED -> pass(started);
			case BUSY -> passed = false; // another instance is wiping
			case WIPE -> {
				passed = false;
				detected.increment();
				OptionalInt deleted = stock.wipeAllEvents(owner, WIPE_LEASE);
				if (deleted.isEmpty() || !stock.trustServer(server, owner)) {
					log.warn("Lost the Redis wipe lock (its lease ran out); the instance that took it over finishes");
					return;
				}
				pass(started);
				log.error("Redis server changed (now {}): its event state may be old, deleted {} keys to rebuild "
						+ "from MySQL", server, deleted.getAsInt());
			}
		}
	}

	/** The verdict is as old as the moment its check started, not when it finished. */
	private void pass(long startedNanos) {
		passedAtNanos = startedNanos;
		passed = true;
	}

}
