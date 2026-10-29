package com.yitaihuang.ticketrush.reservation;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.yitaihuang.ticketrush.common.ApiException;
import com.yitaihuang.ticketrush.event.EventCatalog;
import com.yitaihuang.ticketrush.event.EventSnapshot;
import com.yitaihuang.ticketrush.order.OrderRequested;
import com.yitaihuang.ticketrush.order.PendingOrderTracker;
import com.yitaihuang.ticketrush.reservation.StockGateway.Reservation;

import io.micrometer.core.instrument.MeterRegistry;
import tools.jackson.databind.json.JsonMapper;

/**
 * The hot path. A reservation never touches MySQL: Redis decides whether the
 * user gets tickets and records the order, Kafka durably queues it, and the
 * client polls for the final order. This keeps latency flat while traffic spikes.
 */
@Service
public class ReservationService {

	private static final Logger log = LoggerFactory.getLogger(ReservationService.class);

	private final EventCatalog catalog;

	private final StockGateway stock;

	private final StockRecovery recovery;

	private final RateLimiter rateLimiter;

	private final OrderRequestPublisher publisher;

	private final PendingOrderTracker pendingOrders;

	private final JsonMapper json;

	private final MeterRegistry meters;

	private final Clock clock;

	public ReservationService(EventCatalog catalog, StockGateway stock, StockRecovery recovery,
			RateLimiter rateLimiter, OrderRequestPublisher publisher, PendingOrderTracker pendingOrders,
			JsonMapper json, MeterRegistry meters, Clock clock) {
		this.catalog = catalog;
		this.stock = stock;
		this.recovery = recovery;
		this.rateLimiter = rateLimiter;
		this.publisher = publisher;
		this.pendingOrders = pendingOrders;
		this.json = json;
		this.meters = meters;
		this.clock = clock;
	}

	public record Result(String orderId, boolean replayed) {
	}

	public Result reserve(long eventId, String userId, int quantity, String idempotencyKey) {
		if (!rateLimiter.tryAcquire(userId)) {
			throw rejected("rate_limited", ApiException.tooManyRequests("RATE_LIMITED", "Too many requests, slow down"));
		}

		if (!recovery.redisTrusted()) {
			// Default deny: this instance hasn't confirmed yet that Redis's state isn't an old copy.
			throw rejected("stock_recovering", ApiException.serviceUnavailable("STOCK_RECOVERING",
					"Ticket inventory is being restored, retry shortly"));
		}

		EventSnapshot event = catalog.get(eventId);
		Instant now = clock.instant();
		if (now.isBefore(event.saleStartsAt())) {
			throw rejected("sale_not_started",
					ApiException.conflict("SALE_NOT_STARTED", "Ticket sales open at " + event.saleStartsAt()));
		}

		// The order id and message are fixed before Redis sees them, so every retry
		// and republish of this reservation carries the same identity.
		String orderId = UUID.randomUUID().toString();
		String orderJson = json.writeValueAsString(new OrderRequested(orderId, eventId, userId, quantity,
				quantity * event.priceCents(), idempotencyKey, now));
		Reservation reservation = stock.reserve(eventId, userId, quantity, event.perUserLimit(), idempotencyKey,
				orderId, orderJson);
		if (reservation.outcome() == StockGateway.Outcome.NOT_INITIALIZED && recovery.rebuild(eventId)) {
			reservation = stock.reserve(eventId, userId, quantity, event.perUserLimit(), idempotencyKey, orderId,
					orderJson);
		}

		return switch (reservation.outcome()) {
			case RESERVED -> handOff(eventId, userId, idempotencyKey, orderId, orderJson);
			case DUPLICATE -> {
				// The original may still be on its way to Kafka (its reply was lost); polling shows it as processing.
				pendingOrders.markProcessing(reservation.orderId(), userId);
				count("duplicate");
				yield new Result(reservation.orderId(), true);
			}
			case SOLD_OUT -> throw rejected("sold_out", ApiException.conflict("SOLD_OUT", "No tickets left"));
			case LIMIT_EXCEEDED -> throw rejected("limit_exceeded", ApiException.conflict("PURCHASE_LIMIT_EXCEEDED",
					"At most " + event.perUserLimit() + " tickets per customer"));
			case QUEUE_BACKLOGGED -> throw rejected("queue_backlogged", ApiException
				.serviceUnavailable("ORDER_QUEUE_UNAVAILABLE", "Orders are not being confirmed right now, retry"));
			case NOT_INITIALIZED -> throw rejected("stock_recovering", ApiException
				.serviceUnavailable("STOCK_RECOVERING", "Ticket inventory is being restored, retry shortly"));
		};
	}

	private Result handOff(long eventId, String userId, String idempotencyKey, String orderId, String orderJson) {
		switch (publisher.publish(eventId, orderId, orderJson)) {
			case ACKED -> stock.markPublished(eventId, orderId);
			case NOT_SENT -> {
				// Our send never left the producer. Unless the relay or the consumer has
				// taken the order meanwhile (then a copy may be in Kafka, and the release
				// is refused), nobody will persist it: give the tickets back.
				if (stock.releaseAndForget(eventId, orderId, userId, idempotencyKey)) {
					throw rejected("queue_unavailable", ApiException.serviceUnavailable("ORDER_QUEUE_UNAVAILABLE",
							"Could not place the order, retry"));
				}
				count("unconfirmed");
				log.warn("Order {} was not sent by this request but was already taken by the relay", orderId);
			}
			case UNKNOWN -> {
				// Kafka may already have it. Releasing now could sell these tickets twice;
				// the order stays in the outbox and the relay sends it again under the same id.
				count("unconfirmed");
				log.warn("Order {} accepted without a Kafka ack; left to the outbox relay", orderId);
			}
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
