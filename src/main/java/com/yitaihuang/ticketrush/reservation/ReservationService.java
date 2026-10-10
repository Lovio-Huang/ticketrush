package com.yitaihuang.ticketrush.reservation;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.yitaihuang.ticketrush.common.ApiException;
import com.yitaihuang.ticketrush.event.Event;
import com.yitaihuang.ticketrush.event.EventCatalog;
import com.yitaihuang.ticketrush.event.EventRepository;
import com.yitaihuang.ticketrush.event.EventSnapshot;
import com.yitaihuang.ticketrush.order.OrderRequested;
import com.yitaihuang.ticketrush.order.PendingOrderTracker;
import com.yitaihuang.ticketrush.reservation.StockGateway.Reservation;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * The hot path. A reservation never touches MySQL: Redis decides whether the
 * user gets tickets, Kafka durably queues the order, and the client polls for
 * the final order. This keeps latency flat while traffic spikes.
 */
@Service
public class ReservationService {

	private static final Logger log = LoggerFactory.getLogger(ReservationService.class);

	private final EventCatalog catalog;

	private final EventRepository events;

	private final StockGateway stock;

	private final RateLimiter rateLimiter;

	private final OrderRequestPublisher publisher;

	private final PendingOrderTracker pendingOrders;

	private final MeterRegistry meters;

	private final Clock clock;

	public ReservationService(EventCatalog catalog, EventRepository events, StockGateway stock,
			RateLimiter rateLimiter, OrderRequestPublisher publisher, PendingOrderTracker pendingOrders,
			MeterRegistry meters, Clock clock) {
		this.catalog = catalog;
		this.events = events;
		this.stock = stock;
		this.rateLimiter = rateLimiter;
		this.publisher = publisher;
		this.pendingOrders = pendingOrders;
		this.meters = meters;
		this.clock = clock;
	}

	public record Result(String orderId, boolean replayed) {
	}

	public Result reserve(long eventId, String userId, int quantity, String idempotencyKey) {
		if (!rateLimiter.tryAcquire(userId)) {
			throw rejected("rate_limited", ApiException.tooManyRequests("RATE_LIMITED", "Too many requests, slow down"));
		}

		EventSnapshot event = catalog.get(eventId);
		Instant now = clock.instant();
		if (now.isBefore(event.saleStartsAt())) {
			throw rejected("sale_not_started",
					ApiException.conflict("SALE_NOT_STARTED", "Ticket sales open at " + event.saleStartsAt()));
		}

		String orderId = UUID.randomUUID().toString();
		Reservation reservation = stock.reserve(eventId, userId, quantity, event.perUserLimit(), idempotencyKey,
				orderId);
		if (reservation.outcome() == StockGateway.Outcome.NOT_INITIALIZED) {
			// Redis lost its data (restart, eviction). MySQL is the source of truth.
			int available = events.findById(eventId).map(Event::getAvailableTickets).orElse(0);
			stock.initializeIfAbsent(eventId, available);
			reservation = stock.reserve(eventId, userId, quantity, event.perUserLimit(), idempotencyKey, orderId);
		}

		return switch (reservation.outcome()) {
			case RESERVED -> handOff(event, userId, quantity, idempotencyKey, orderId, now);
			case DUPLICATE -> {
				count("duplicate");
				yield new Result(reservation.orderId(), true);
			}
			case SOLD_OUT -> throw rejected("sold_out", ApiException.conflict("SOLD_OUT", "No tickets left"));
			case LIMIT_EXCEEDED -> throw rejected("limit_exceeded", ApiException.conflict("PURCHASE_LIMIT_EXCEEDED",
					"At most " + event.perUserLimit() + " tickets per customer"));
			case NOT_INITIALIZED -> throw new IllegalStateException("Stock for event " + eventId + " is missing");
		};
	}

	private Result handOff(EventSnapshot event, String userId, int quantity, String idempotencyKey, String orderId,
			Instant now) {
		var request = new OrderRequested(orderId, event.id(), userId, quantity, quantity * event.priceCents(),
				idempotencyKey, now);
		try {
			publisher.publish(request);
		}
		catch (RuntimeException ex) {
			// Nobody will ever persist this order, so give the tickets back.
			stock.releaseAndForget(event.id(), userId, quantity, idempotencyKey);
			log.error("Could not queue order {} for event {}", orderId, event.id(), ex);
			throw rejected("queue_unavailable",
					ApiException.serviceUnavailable("ORDER_QUEUE_UNAVAILABLE", "Could not place the order, retry"));
		}
		pendingOrders.markProcessing(orderId, userId);
		count("accepted");
		return new Result(orderId, false);
	}

	private ApiException rejected(String outcome, ApiException ex) {
		count(outcome);
		return ex;
	}

	private void count(String outcome) {
		meters.counter("ticketrush.reservations", "outcome", outcome).increment();
	}

}
