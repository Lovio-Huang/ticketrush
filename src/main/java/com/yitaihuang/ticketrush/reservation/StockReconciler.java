package com.yitaihuang.ticketrush.reservation;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.event.Event;
import com.yitaihuang.ticketrush.event.EventRepository;
import com.yitaihuang.ticketrush.order.Order;
import com.yitaihuang.ticketrush.order.OrderRepository;
import com.yitaihuang.ticketrush.reservation.StockGateway.Recount;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Self-healing for the Redis copy of the inventory.
 *
 * <p>MySQL's stock and Redis's stock legitimately differ by the orders in
 * flight (reserved in Redis, not yet written by the consumer), so copying
 * MySQL's number into Redis would put those tickets on sale twice. Instead the
 * job relies on two invariants that every Redis script keeps:
 *
 * <ul>
 * <li>{@code stock + tickets of held orders = total tickets}, and per-user
 * counts equal the tickets each user holds;</li>
 * <li>an order is released at most once, keyed by its id.</li>
 * </ul>
 *
 * Each run, per event on sale or not yet settled: a missing stock key triggers a rebuild; orders
 * Redis still holds but MySQL has finished (cancelled, expired, rejected, e.g.
 * after a crash right after the commit) are released; then, if the O(1)
 * stock-count invariant check {@code stock + held = total} fails, stock and
 * per-user counts are recounted from the held orders in one atomic script. That
 * check covers the two counters only, not the orders hash, per-user counts or
 * agreement with MySQL: it is not an audit. Every step is either idempotent
 * per order or atomic, so it runs safely during a sale, with the consumer
 * paused, and on several instances at once.
 *
 * <p>The full recount is gated because it blocks Redis for its whole run
 * (measured: ~3 ms per 2k held orders, ~34 ms per 20k, ~220 ms per 100k). The
 * held-order scan above uses HSCAN, which doesn't block, and only proposes
 * candidates: every release it triggers is idempotent and only for orders
 * MySQL has finished, so an order added or released mid-scan is at worst
 * picked up on the next run.
 *
 * <p>An event's sale ends when it starts, but orders accepted before that may
 * still be in the outbox, and cancellations, expiries, dead letters and voids
 * still change its tickets. So an event stays in the jobs' list until it is
 * settled: {@code settleAfter} past its start, a run found nothing to fix, and
 * its outbox is empty (no order can join it after the start, by Redis's clock).
 * Everything else that may need the jobs later is a MySQL write that reopens
 * the event (EventRepository#touch); the settle is a compare-and-set on the
 * version read before the checks, so a write that lands during them keeps the
 * event in, on any number of instances.
 */
@Component
public class StockReconciler {

	private static final Logger log = LoggerFactory.getLogger(StockReconciler.class);

	private static final int BATCH = 500;

	private final EventRepository events;

	private final OrderRepository orders;

	private final StockGateway stock;

	private final StockRecovery recovery;

	private final MeterRegistry meters;

	private final Clock clock;

	private final Duration settleAfter;

	public StockReconciler(EventRepository events, OrderRepository orders, StockGateway stock, StockRecovery recovery,
			TicketrushProperties properties, MeterRegistry meters, Clock clock) {
		this.events = events;
		this.orders = orders;
		this.stock = stock;
		this.recovery = recovery;
		this.settleAfter = properties.reconciliation().settleAfter();
		this.meters = meters;
		this.clock = clock;
	}

	@Scheduled(fixedDelayString = "${ticketrush.reconciliation.interval}",
			initialDelayString = "${ticketrush.reconciliation.interval}")
	public void run() {
		Instant now = clock.instant();
		for (Event event : events.findNeedingUpkeep(now)) {
			// The version was read with the event, before any check: see markSettled.
			if (reconcile(event) && settled(event, now)
					&& events.markSettled(event.getId(), event.getUpkeepVersion(), now) == 1) {
				log.info("Event {} is settled: no accepted order needs the background jobs any more", event.getId());
			}
		}
	}

	/** Brings the event's Redis state in line; true if it found nothing to fix. */
	boolean reconcile(Event event) {
		long eventId = event.getId();
		if (stock.available(eventId).isEmpty()) {
			recovery.rebuild(eventId);
			return false;
		}
		int released = releaseFinishedOrders(eventId);
		if (!stock.drifted(eventId, event.getTotalTickets())) {
			return released == 0;
		}
		Recount recount = stock.recount(eventId, event.getTotalTickets());
		if (recount.changed()) {
			meters.counter("ticketrush.stock.corrections").increment();
			log.warn("Recounted Redis stock of event {} from {} to {} (per-user counts changed: {})", eventId,
					recount.previous(), recount.stock(), recount.buyersChanged());
		}
		return false;
	}

	private boolean settled(Event event, Instant now) {
		return !now.isBefore(event.getStartsAt().plus(settleAfter)) && stock.outboxSize(event.getId()) == 0;
	}

	/** Releases orders Redis still holds although MySQL no longer gives them tickets; returns how many. */
	private int releaseFinishedOrders(long eventId) {
		int released = 0;
		List<String> held = stock.heldOrderIds(eventId);
		for (int from = 0; from < held.size(); from += BATCH) {
			for (Order order : orders.findAllById(held.subList(from, Math.min(from + BATCH, held.size())))) {
				if (!order.getStatus().holdsTickets() && stock.release(eventId, order.getId())) {
					released++;
					meters.counter("ticketrush.stock.late_releases").increment();
					log.warn("Released order {} ({}) that Redis still held", order.getId(), order.getStatus());
				}
			}
		}
		return released;
	}

}
