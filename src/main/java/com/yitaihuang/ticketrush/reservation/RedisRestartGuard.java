package com.yitaihuang.ticketrush.reservation;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
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
 * stock first, and the fenced rebuild recreates them from MySQL. A missing
 * record (a fresh or emptied Redis) counts as a mismatch. The record travels
 * with the data, so a snapshot restored while every app was down carries the
 * old server's id and is caught by the first check.
 *
 * <p>Until a process's latest check has passed it refuses reservations and
 * rebuilds ({@link #verified}), so it never trusts state before checking it, nor
 * while another instance is still wiping. What stays open: a running instance
 * keeps its last verdict until its next check, so for up to about a second after
 * its Redis is replaced by one with older data it still sells on that state.
 * Compose runs Redis without persistence, so a restart there comes back empty
 * and is refused at once.
 */
@Component
public class RedisRestartGuard {

	private static final Logger log = LoggerFactory.getLogger(RedisRestartGuard.class);

	/** The run_id of the server whose state the app last trusted. */
	public static final String SERVER_KEY = "ticketrush:redis-server";

	public static final String WIPE_LOCK = "ticketrush:redis-server:wiping";

	/** Long enough to wipe every event; if the wiper dies, the next check after it redoes the wipe. */
	private static final Duration WIPE_LEASE = Duration.ofMinutes(1);

	private final StockGateway stock;

	private final Counter detected;

	/**
	 * Default deny: false until the first check, and whenever the last one found a
	 * changed server. Reservations and rebuilds wait for it, so a process that just
	 * started on top of a restored snapshot, or that runs while another instance is
	 * (re)doing a wipe, can't sell on, or rebuild into, state that isn't trusted.
	 * It gates this process only; the wipe itself is decided once, in Redis.
	 */
	private volatile boolean verified;

	public RedisRestartGuard(StockGateway stock, MeterRegistry meters) {
		this.stock = stock;
		this.detected = meters.counter("ticketrush.redis.restarts_detected");
	}

	@Scheduled(fixedDelayString = "${ticketrush.reservations.redis-check-interval:PT1S}")
	public void run() {
		try {
			check();
		}
		catch (RuntimeException ex) {
			verified = false;
			log.warn("Could not check the Redis server: {}", ex.toString());
		}
	}

	/** True only while this process's latest check found the current server trusted. */
	public boolean verified() {
		return verified;
	}

	@EventListener(ApplicationReadyEvent.class)
	public void checkAtStartup() {
		run();
	}

	public void check() {
		String server = stock.serverRunId();
		switch (stock.checkServer(server, WIPE_LEASE)) {
			case TRUSTED -> verified = true;
			case BUSY -> verified = false; // another instance is wiping
			case WIPE -> {
				verified = false;
				detected.increment();
				int deleted = stock.wipeAllEvents();
				stock.trustServer(server);
				verified = true;
				log.error("Redis server changed (now {}): its event state may be old, deleted {} keys to rebuild "
						+ "from MySQL", server, deleted);
			}
		}
	}

}
