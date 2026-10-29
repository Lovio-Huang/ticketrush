package com.yitaihuang.ticketrush.reservation;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.yitaihuang.ticketrush.event.Event;
import com.yitaihuang.ticketrush.event.EventRepository;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Self-healing for the Redis copy of the inventory.
 *
 * <p>Redis and MySQL can drift apart: the app may crash between committing an
 * expiry in MySQL and returning the tickets to Redis (Redis too low, tickets go
 * unsold), or someone may edit Redis by hand (Redis too high, MySQL rejects the
 * extra orders). This job puts MySQL's number back into Redis, but only when it
 * is sure the difference is real drift and not orders in flight:
 *
 * <ol>
 * <li>the Kafka order queue must be fully drained (consumer lag 0), and</li>
 * <li>the same mismatch must be seen on two consecutive runs with nothing having
 * changed in between, so a reservation that is mid-flight can't be mistaken
 * for drift.</li>
 * </ol>
 *
 * The fix itself is a compare-and-set, so it never overwrites a reservation
 * that slipped in after the check.
 */
@Component
public class StockReconciler {

	private static final Logger log = LoggerFactory.getLogger(StockReconciler.class);

	record Observation(int redis, int database) {
	}

	private final EventRepository events;

	private final StockGateway stock;

	private final OrderQueueLag queueLag;

	private final MeterRegistry meters;

	private final Clock clock;

	/** Mismatches seen on the previous run, by event id. */
	private final Map<Long, Observation> suspected = new ConcurrentHashMap<>();

	public StockReconciler(EventRepository events, StockGateway stock, OrderQueueLag queueLag, MeterRegistry meters,
			Clock clock) {
		this.events = events;
		this.stock = stock;
		this.queueLag = queueLag;
		this.meters = meters;
		this.clock = clock;
	}

	@Scheduled(fixedDelayString = "${ticketrush.reconciliation.interval}",
			initialDelayString = "${ticketrush.reconciliation.interval}")
	public void run() {
		OptionalLong lag = queueLag.current();
		if (lag.isEmpty() || lag.getAsLong() > 0) {
			suspected.clear();
			return;
		}
		List<Event> onSale = events.findOnSale(clock.instant());
		List<Long> ids = onSale.stream().map(Event::getId).toList();
		Map<Long, Integer> inRedis = stock.availableMany(ids);
		for (Event event : onSale) {
			check(event.getId(), event.getAvailableTickets(), inRedis.get(event.getId()));
		}
		suspected.keySet().retainAll(ids);
	}

	void check(long eventId, int inDatabase, Integer inRedis) {
		if (inRedis == null) {
			stock.initializeIfAbsent(eventId, inDatabase);
			return;
		}
		if (inRedis == inDatabase) {
			suspected.remove(eventId);
			return;
		}
		Observation now = new Observation(inRedis, inDatabase);
		Observation before = suspected.put(eventId, now);
		if (!now.equals(before)) {
			return; // first sighting, or the numbers are still moving: look again next run
		}
		if (stock.compareAndSet(eventId, inRedis, inDatabase)) {
			suspected.remove(eventId);
			meters.counter("ticketrush.stock.corrections").increment();
			log.warn("Corrected Redis stock for event {} from {} to {} to match MySQL", eventId, inRedis, inDatabase);
		}
	}

}
