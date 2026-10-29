package com.yitaihuang.ticketrush.reservation;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.event.Event;
import com.yitaihuang.ticketrush.event.EventRepository;
import com.yitaihuang.ticketrush.order.Order;
import com.yitaihuang.ticketrush.order.OrderRepository;
import com.yitaihuang.ticketrush.order.OrderRequested;
import com.yitaihuang.ticketrush.reservation.StockGateway.Recount;
import com.yitaihuang.ticketrush.reservation.StockGateway.SeedOrder;

import io.micrometer.core.instrument.MeterRegistry;
import tools.jackson.databind.json.JsonMapper;

/**
 * Rebuilds an event's Redis state from MySQL after Redis lost it (restart
 * without persistence, flush). Stock alone is not enough: held orders give the
 * per-user counts, and idempotency keys stop a client's retry from becoming a
 * second order.
 *
 * <p>While the event's stock key is missing, reservations for it are refused
 * (503, retryable). The barrier before rebuilding has two parts:
 * <ol>
 * <li>a quiet period from the moment the loss was first noticed, longer than a
 * send can stay in flight (max.block.ms + delivery.timeout.ms), so requests and
 * relay batches that started before the loss, on any instance, have either
 * reached Kafka or failed for good;</li>
 * <li>then consumer lag 0, so everything that did reach Kafka is in MySQL.</li>
 * </ol>
 * After that MySQL knows every order whose send had reached the producer, but
 * not one whose request paused before its send for longer than any timer. Such
 * an order arrives after the rebuild, unknown to Redis while the stock key
 * exists, and the consumer rejects it (see confirm.lua); that is sound because
 * {@link RedisRestartGuard} turns every restart into a full loss, so Redis never
 * runs on a partial old state. Orders only Redis knew are gone with Redis, and a
 * retry with their idempotency key places a fresh order.
 *
 * <p>The rebuild runs under a lease identified by a random token, checked inside
 * every seeding script and the final recount, so a rebuilder that stalled past
 * its lease (or whose Redis was wiped again) can't write into a newer rebuild.
 * A crashed rebuild leaves its lease to expire; the next one re-seeds, which is
 * idempotent. Orders consumed meanwhile are claimed into the same hash.
 */
@Component
public class StockRecovery {

	private static final Logger log = LoggerFactory.getLogger(StockRecovery.class);

	private static final Duration LEASE = Duration.ofSeconds(30);

	/** While orders are still in Kafka, ask again after this long rather than on every request. */
	private static final Duration RETRY_AFTER = Duration.ofSeconds(1);

	private static final int BATCH = 500;

	private final EventRepository events;

	private final OrderRepository orders;

	private final StockGateway stock;

	private final OrderQueueLag queueLag;

	private final JsonMapper json;

	private final Duration idempotencyTtl;

	private final Duration quietPeriod;

	private final MeterRegistry meters;

	private final Clock clock;

	private final RedisRestartGuard guard;

	public StockRecovery(EventRepository events, OrderRepository orders, StockGateway stock, OrderQueueLag queueLag,
			JsonMapper json, TicketrushProperties properties, MeterRegistry meters, Clock clock,
			RedisRestartGuard guard) {
		this.guard = guard;
		this.events = events;
		this.orders = orders;
		this.stock = stock;
		this.queueLag = queueLag;
		this.json = json;
		this.idempotencyTtl = properties.reservations().idempotencyTtl();
		this.quietPeriod = properties.reservations().recoveryQuietPeriod();
		this.meters = meters;
		this.clock = clock;
	}

	/** False until this instance has confirmed that the Redis server's state can be trusted (see RedisRestartGuard). */
	public boolean redisTrusted() {
		return guard.verified();
	}

	/** Returns true once the event's Redis state exists again, false if it isn't safe (or this caller's turn) yet. */
	public boolean rebuild(long eventId) {
		if (!guard.verified()) {
			return false; // the Redis server isn't trusted (yet): a wipe may still be coming
		}
		String token = UUID.randomUUID().toString();
		switch (stock.beginRebuild(eventId, token, LEASE, quietPeriod)) {
			case DONE:
				return true;
			case BUSY:
			case WAIT:
				return false;
			case GO:
				break;
		}
		OptionalLong lag = queueLag.current();
		if (lag.isEmpty() || lag.getAsLong() > 0) {
			stock.releaseRebuildLease(eventId, token, RETRY_AFTER);
			return false;
		}
		try {
			return seedAndFinish(eventId, token);
		}
		catch (RuntimeException ex) {
			stock.releaseRebuildLease(eventId, token, Duration.ZERO);
			throw ex;
		}
	}

	private boolean seedAndFinish(long eventId, String token) {
		Event event = events.findById(eventId).orElseThrow();
		Instant now = clock.instant();
		List<SeedOrder> batch = new ArrayList<>(BATCH);
		int seeded = 0;
		for (Order order : orders.findByEventId(eventId)) {
			batch.add(seedOf(order, now));
			if (batch.size() == BATCH) {
				if (!stock.seed(eventId, token, LEASE, batch)) {
					return fencedOff(eventId);
				}
				seeded += batch.size();
				batch.clear();
			}
		}
		if (!stock.seed(eventId, token, LEASE, batch)) {
			return fencedOff(eventId);
		}
		seeded += batch.size();
		Optional<Recount> recount = stock.finishRebuild(eventId, token, event.getTotalTickets());
		if (recount.isEmpty()) {
			return fencedOff(eventId);
		}
		meters.counter("ticketrush.stock.rebuilds").increment();
		log.warn("Rebuilt Redis state of event {} from {} MySQL orders: stock {}", eventId, seeded,
				recount.get().stock());
		return true;
	}

	private boolean fencedOff(long eventId) {
		log.warn("Rebuild of event {} lost its lease; leaving it to the current holder", eventId);
		return false;
	}

	private SeedOrder seedOf(Order order, Instant now) {
		String orderJson = order.getStatus().holdsTickets() ? json.writeValueAsString(OrderRequested.of(order)) : null;
		return new SeedOrder(order.getId(), order.getUserId(), order.getIdempotencyKey(), orderJson,
				Duration.between(now, order.getCreatedAt().plus(idempotencyTtl)));
	}

}
