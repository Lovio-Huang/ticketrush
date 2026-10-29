package com.yitaihuang.ticketrush.order;

import java.time.Clock;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.yitaihuang.ticketrush.common.AfterCommit;
import com.yitaihuang.ticketrush.common.ApiException;
import com.yitaihuang.ticketrush.order.OrderFailure.Status;
import com.yitaihuang.ticketrush.reservation.OrderRequestPublisher;
import com.yitaihuang.ticketrush.reservation.OrderRequestPublisher.Delivery;
import com.yitaihuang.ticketrush.reservation.StockGateway;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Dead-lettered order messages: recorded, counted, and resolved on purpose.
 *
 * <p>While a failure is open its order (if it is one) keeps its tickets held in
 * Redis: nothing is oversold, and nothing is guessed. An operator then either
 * <b>replays</b> it (sends the same message again, e.g. after fixing whatever
 * made it fail), <b>voids</b> it (records it as a rejected order and releases
 * its tickets), or <b>discards</b> it (only when that can't leak tickets: the
 * message isn't an order at all, or the order already exists in MySQL). Every
 * action is idempotent, and a void is final: the rejected row (or, if none can
 * be written, the released marker in Redis) means a copy of the message that
 * turns up later can never become an active order.
 */
@Service
public class OrderFailureService {

	private static final Logger log = LoggerFactory.getLogger(OrderFailureService.class);

	private final OrderFailureRepository failures;

	private final OrderRepository orders;

	private final StockGateway stock;

	private final OrderRequestPublisher publisher;

	private final JsonMapper json;

	private final TransactionTemplate ownTransaction;

	private final Counter deadLettered;

	private final Clock clock;

	public OrderFailureService(OrderFailureRepository failures, OrderRepository orders, StockGateway stock,
			OrderRequestPublisher publisher, JsonMapper json, PlatformTransactionManager transactions,
			MeterRegistry meters, Clock clock) {
		this.failures = failures;
		this.orders = orders;
		this.ownTransaction = new TransactionTemplate(transactions);
		this.ownTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
		this.stock = stock;
		this.publisher = publisher;
		this.json = json;
		this.clock = clock;
		this.deadLettered = meters.counter("ticketrush.orders.dead_lettered");
		Gauge.builder("ticketrush.orders.failures.open", failures, repo -> repo.countByStatus(Status.OPEN))
			.register(meters);
	}

	/** Records one dead letter. Idempotent per DLT record; repeats of the same order bump its attempt count. */
	@Transactional
	public void record(String payload, String error, String dltPosition) {
		if (failures.existsByDltPositionOrLastDltPosition(dltPosition, dltPosition)) {
			return; // the DLT listener got this exact record again
		}
		OrderRequested request = parse(payload).orElse(null);
		Optional<OrderFailure> known = request == null ? Optional.empty() : failures.lockByOrderId(request.orderId());
		if (known.isPresent()) {
			OrderFailure failure = known.get();
			failure.failedAgain(error, dltPosition, clock.instant());
			if (failure.getStatus() == Status.VOIDED) {
				// A late copy of a voided order: make sure it holds nothing (Redis may have re-held it).
				AfterCommit.run(() -> stock.release(request.eventId(), request.orderId()));
			}
			log.warn("Order {} dead-lettered again ({} attempts, {})", request.orderId(), failure.getAttempts(),
					failure.getStatus());
		}
		else {
			failures.save(new OrderFailure(request, payload, error, dltPosition, clock.instant()));
			log.error("Order message dead-lettered at {} (order {}): {}", dltPosition,
					request == null ? "unparseable" : request.orderId(), error);
		}
		deadLettered.increment();
	}

	@Transactional(readOnly = true)
	public List<OrderFailure> list(Status status) {
		return status == null ? failures.findTop100ByOrderByIdDesc() : failures.findTop100ByStatusOrderByIdDesc(status);
	}

	/** Sends the original message to the order topic again, under the same order id. */
	@Transactional
	public OrderFailure replay(long id, String note) {
		OrderFailure failure = lock(id);
		if (failure.isFinal()) {
			throw ApiException.conflict("FAILURE_RESOLVED", "Failure " + id + " is already " + failure.getStatus());
		}
		OrderRequested request = requireOrder(failure);
		if (publisher.publish(request.eventId(), request.orderId(), failure.getPayload()) != Delivery.ACKED) {
			throw ApiException.serviceUnavailable("ORDER_QUEUE_UNAVAILABLE", "Kafka did not confirm the replay, retry");
		}
		failure.resolve(Status.REPLAYED, note, clock.instant());
		return failure;
	}

	/** Gives the order up for good: a rejected order row where possible, and its tickets released. */
	@Transactional
	public OrderFailure voidOrder(long id, String note) {
		OrderFailure failure = lock(id);
		if (failure.getStatus() == Status.VOIDED) {
			return failure;
		}
		if (failure.isFinal()) {
			throw ApiException.conflict("FAILURE_RESOLVED", "Failure " + id + " is already " + failure.getStatus());
		}
		OrderRequested request = requireOrder(failure);
		Optional<Order> stored = orders.findById(request.orderId());
		if (stored.isPresent() && stored.get().getStatus().holdsTickets()) {
			throw ApiException.conflict("ORDER_ACTIVE",
					"Order " + request.orderId() + " is " + stored.get().getStatus() + "; cancel it instead");
		}
		if (stored.isEmpty()) {
			recordRejected(request);
		}
		failure.resolve(Status.VOIDED, note, clock.instant());
		AfterCommit.run(() -> stock.release(request.eventId(), request.orderId()));
		log.warn("Voided dead-lettered order {}", request.orderId());
		return failure;
	}

	/**
	 * A final row, so a copy of the message that arrives later is a redelivery of
	 * a rejected order. Its own transaction: the same constraint that made the
	 * message fail (foreign key, idempotency clash, a field too long) may refuse
	 * this row too. Then the released marker in Redis is what keeps a late copy
	 * from taking tickets (the consumer rejects released orders), and that copy
	 * fails again on the same constraint anyway.
	 */
	private void recordRejected(OrderRequested request) {
		try {
			ownTransaction.executeWithoutResult(status -> orders.save(Order.rejected(request, clock.instant())));
		}
		catch (DataIntegrityViolationException ex) {
			// The consumer of another copy may have stored it since we looked: then it is a
			// live order (cancel it instead), and releasing its tickets would sell them twice.
			Optional<Order> stored = ownTransaction.execute(status -> orders.findById(request.orderId()));
			if (stored.isPresent() && stored.get().getStatus().holdsTickets()) {
				throw ApiException.conflict("ORDER_ACTIVE",
						"Order " + request.orderId() + " is " + stored.get().getStatus() + "; cancel it instead");
			}
			log.warn("Voided order {} can't be stored as rejected ({}); relying on Redis", request.orderId(),
					ex.getMostSpecificCause().getMessage());
		}
	}

	/** Acknowledges a failure without touching stock, when that can't leave tickets held. */
	@Transactional
	public OrderFailure discard(long id, String note) {
		OrderFailure failure = lock(id);
		if (failure.getStatus() == Status.DISCARDED) {
			return failure;
		}
		if (failure.isFinal()) {
			throw ApiException.conflict("FAILURE_RESOLVED", "Failure " + id + " is already " + failure.getStatus());
		}
		if (failure.getOrderId() != null && !orders.existsById(failure.getOrderId())) {
			throw ApiException.conflict("ORDER_HOLDS_TICKETS",
					"Order " + failure.getOrderId() + " may still hold tickets; replay or void it");
		}
		failure.resolve(Status.DISCARDED, note, clock.instant());
		return failure;
	}

	private OrderFailure lock(long id) {
		return failures.lockById(id)
			.orElseThrow(() -> ApiException.notFound("FAILURE_NOT_FOUND", "Failure " + id + " does not exist"));
	}

	private OrderRequested requireOrder(OrderFailure failure) {
		return parse(failure.getPayload()).orElseThrow(() -> ApiException.conflict("NOT_AN_ORDER",
				"Failure " + failure.getId() + " is not a readable order; discard it"));
	}

	private Optional<OrderRequested> parse(String payload) {
		try {
			// A JSON null ("null") reads as no object at all: unreadable, like any other non-order.
			OrderRequested request = json.readValue(payload, OrderRequested.class);
			// Our order ids are UUIDs (36 chars, the column size): a longer one can't be an order that holds tickets.
			return request == null || request.orderId() == null || request.orderId().length() > 36 || request.userId() == null
					|| request.idempotencyKey() == null ? Optional.empty() : Optional.of(request);
		}
		catch (JacksonException | IllegalArgumentException ex) {
			return Optional.empty();
		}
	}

}
