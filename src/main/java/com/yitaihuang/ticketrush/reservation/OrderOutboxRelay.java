package com.yitaihuang.ticketrush.reservation;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.event.Event;
import com.yitaihuang.ticketrush.event.EventRepository;
import com.yitaihuang.ticketrush.reservation.OrderRequestPublisher.Delivery;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Sends accepted orders that Kafka never confirmed: an ack that timed out, or
 * an instance that crashed between reserving in Redis and publishing. Redis
 * recorded each one atomically with its reservation (the outbox), so nothing
 * accepted is lost and the hot path gains no database write.
 *
 * <p>Resending is safe because the order id is fixed at reservation time and
 * the consumer is idempotent by order id. An order leaves the outbox when Kafka
 * acks it or when the consumer has stored it, whichever comes first.
 */
@Component
public class OrderOutboxRelay {

	private static final Logger log = LoggerFactory.getLogger(OrderOutboxRelay.class);

	private static final int BATCH = 100;

	private final EventRepository events;

	private final StockGateway stock;

	private final OrderRequestPublisher publisher;

	private final Duration republishAfter;

	private final MeterRegistry meters;

	private final Clock clock;

	public OrderOutboxRelay(EventRepository events, StockGateway stock, OrderRequestPublisher publisher,
			TicketrushProperties properties, MeterRegistry meters, Clock clock) {
		this.events = events;
		this.stock = stock;
		this.publisher = publisher;
		this.republishAfter = properties.reservations().republishAfter();
		this.meters = meters;
		this.clock = clock;
	}

	@Scheduled(fixedDelayString = "${ticketrush.reservations.relay-interval}",
			initialDelayString = "${ticketrush.reservations.relay-interval}")
	public void run() {
		for (Event event : events.findOnSale(clock.instant())) {
			relay(event.getId());
		}
	}

	/** Republishes one event's overdue orders; returns how many Kafka acknowledged. */
	public int relay(long eventId) {
		int sent = 0;
		for (Map.Entry<String, String> order : stock.dueForRepublish(eventId, republishAfter, BATCH).entrySet()) {
			if (publisher.publish(eventId, order.getKey(), order.getValue()) != Delivery.ACKED) {
				break; // Kafka is still struggling; the rest stay queued for the next round
			}
			stock.markPublished(eventId, order.getKey());
			sent++;
		}
		if (sent > 0) {
			meters.counter("ticketrush.outbox.republished").increment(sent);
			log.warn("Republished {} unconfirmed orders of event {}", sent, eventId);
		}
		return sent;
	}

}
