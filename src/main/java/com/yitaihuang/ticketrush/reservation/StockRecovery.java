package com.yitaihuang.ticketrush.reservation;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

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
 * (503, retryable). The rebuild waits until the order topic is drained, because
 * an order still in Kafka is in neither store: rebuilding before it lands would
 * forget its idempotency key. Orders consumed during the rebuild are confirmed
 * into the same hash, so none is lost either way.
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

	private final MeterRegistry meters;

	private final Clock clock;

	public StockRecovery(EventRepository events, OrderRepository orders, StockGateway stock, OrderQueueLag queueLag,
			JsonMapper json, TicketrushProperties properties, MeterRegistry meters, Clock clock) {
		this.events = events;
		this.orders = orders;
		this.stock = stock;
		this.queueLag = queueLag;
		this.json = json;
		this.idempotencyTtl = properties.reservations().idempotencyTtl();
		this.meters = meters;
		this.clock = clock;
	}

	/** Returns true once the event's Redis state exists again, false if it isn't safe to rebuild yet. */
	public boolean rebuild(long eventId) {
		if (!stock.tryLeaseRebuild(eventId, LEASE)) {
			return false;
		}
		OptionalLong lag = queueLag.current();
		if (lag.isEmpty() || lag.getAsLong() > 0) {
			stock.shortenRebuildLease(eventId, RETRY_AFTER);
			return false;
		}
		try {
			if (stock.available(eventId).isPresent()) {
				return true;
			}
			Event event = events.findById(eventId).orElseThrow();
			Instant now = clock.instant();
			List<SeedOrder> batch = new ArrayList<>(BATCH);
			int seeded = 0;
			for (Order order : orders.findByEventId(eventId)) {
				batch.add(seedOf(order, now));
				if (batch.size() == BATCH) {
					stock.seed(eventId, batch);
					seeded += batch.size();
					batch.clear();
				}
			}
			stock.seed(eventId, batch);
			seeded += batch.size();
			Recount recount = stock.recount(eventId, event.getTotalTickets());
			meters.counter("ticketrush.stock.rebuilds").increment();
			log.warn("Rebuilt Redis state of event {} from {} MySQL orders: stock {}", eventId, seeded,
					recount.stock());
			return true;
		}
		finally {
			stock.endRebuild(eventId);
		}
	}

	private SeedOrder seedOf(Order order, Instant now) {
		String orderJson = order.getStatus().holdsTickets() ? json.writeValueAsString(OrderRequested.of(order)) : null;
		return new SeedOrder(order.getId(), order.getUserId(), order.getIdempotencyKey(), orderJson,
				Duration.between(now, order.getCreatedAt().plus(idempotencyTtl)));
	}

}
