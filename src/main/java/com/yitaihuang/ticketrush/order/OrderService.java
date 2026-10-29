package com.yitaihuang.ticketrush.order;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.yitaihuang.ticketrush.common.AfterCommit;
import com.yitaihuang.ticketrush.common.ApiException;
import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.event.EventRepository;
import com.yitaihuang.ticketrush.reservation.StockGateway;

import tools.jackson.databind.json.JsonMapper;

@Service
public class OrderService {

	private static final Logger log = LoggerFactory.getLogger(OrderService.class);

	private final OrderRepository orders;

	private final EventRepository events;

	private final StockGateway stock;

	private final TicketrushProperties properties;

	private final JsonMapper json;

	private final Clock clock;

	public OrderService(OrderRepository orders, EventRepository events, StockGateway stock,
			TicketrushProperties properties, JsonMapper json, Clock clock) {
		this.orders = orders;
		this.events = events;
		this.stock = stock;
		this.properties = properties;
		this.json = json;
		this.clock = clock;
	}

	/**
	 * Persists an order handed over by Kafka. Kafka delivers at least once, so
	 * this must be idempotent: a redelivered message finds the existing row and
	 * only re-syncs Redis. The same order id with different details is a bug, not
	 * a redelivery, and fails so the message is retried and dead-lettered.
	 */
	@Transactional
	public void recordRequest(OrderRequested request) {
		Optional<Order> existing = orders.findById(request.orderId());
		if (existing.isPresent()) {
			syncRedis(requireSameRequest(existing.get(), request), request);
			return;
		}
		Instant now = clock.instant();
		Order order;
		if (events.decrementAvailable(request.eventId(), request.quantity()) == 0) {
			// Redis said yes but MySQL has no stock left: Redis was out of sync
			// (e.g. it lost data while orders were in flight). MySQL wins.
			log.warn("Rejecting order {}: event {} has no stock in MySQL", request.orderId(), request.eventId());
			order = orders.save(Order.rejected(request, now));
		}
		else {
			order = orders.save(Order.pendingPayment(request, now, properties.orders().paymentWindow()));
		}
		syncRedis(order, request);
	}

	/**
	 * After the insert hit a constraint: true only if this exact order is already
	 * stored (another consumer won a race), in which case Redis is synced as for
	 * any redelivery. Any other constraint error is real and must not be skipped.
	 */
	@Transactional(readOnly = true)
	public boolean acknowledgeReplay(OrderRequested request) {
		Optional<Order> stored = orders.findById(request.orderId()).filter(order -> order.isSameRequest(request));
		stored.ifPresent(order -> syncRedis(order, request));
		return stored.isPresent();
	}

	private static Order requireSameRequest(Order order, OrderRequested request) {
		if (!order.isSameRequest(request)) {
			throw new IllegalStateException("Order " + order.getId() + " already exists with different details");
		}
		return order;
	}

	/**
	 * Brings Redis in line with the order once MySQL has committed: an active
	 * order is confirmed (leaves the outbox, re-held if Redis lost it), any other
	 * gives its tickets back. Both are idempotent, and a failure here fails the
	 * message, so Kafka's redelivery retries it.
	 */
	private void syncRedis(Order order, OrderRequested request) {
		long eventId = order.getEventId();
		if (order.getStatus().holdsTickets()) {
			String orderJson = json.writeValueAsString(request);
			Duration idempotencyLeft = Duration.between(clock.instant(),
					request.requestedAt().plus(properties.reservations().idempotencyTtl()));
			AfterCommit.run(() -> stock.confirm(eventId, order.getId(), order.getUserId(), order.getIdempotencyKey(),
					orderJson, idempotencyLeft));
		}
		else {
			AfterCommit.run(() -> stock.release(eventId, order.getId()));
		}
	}

	@Transactional(readOnly = true)
	public Optional<Order> find(String orderId, String userId) {
		// Someone else's order looks exactly like a missing one: no ID probing.
		return orders.findById(orderId).filter(order -> order.isOwnedBy(userId));
	}

	@Transactional(readOnly = true)
	public Page<Order> history(String userId, OrderStatus status, Pageable pageable) {
		return status == null ? orders.findByUserIdOrderByCreatedAtDesc(userId, pageable)
				: orders.findByUserIdAndStatusOrderByCreatedAtDesc(userId, status, pageable);
	}

	@Transactional
	public Order pay(String orderId, String userId) {
		Order order = require(orderId, userId);
		order.pay(clock.instant());
		return order;
	}

	@Transactional
	public Order cancel(String orderId, String userId) {
		Order order = require(orderId, userId);
		order.cancel(clock.instant());
		returnTickets(order);
		return order;
	}

	/** Expires one batch of unpaid orders and returns how many it expired. */
	@Transactional
	public int expireOverdue(int limit) {
		Instant now = clock.instant();
		List<Order> overdue = orders.lockOverdue(now, limit);
		for (Order order : overdue) {
			order.expire(now);
			returnTickets(order);
		}
		return overdue.size();
	}

	private void returnTickets(Order order) {
		events.incrementAvailable(order.getEventId(), order.getQuantity());
		AfterCommit.run(() -> {
			try {
				stock.release(order.getEventId(), order.getId());
			}
			catch (RuntimeException ex) {
				// MySQL already committed; the reconciler re-applies the (idempotent) release.
				log.warn("Could not return tickets of order {} to Redis yet: {}", order.getId(), ex.toString());
			}
		});
	}

	private Order require(String orderId, String userId) {
		return find(orderId, userId)
			.orElseThrow(() -> ApiException.notFound("ORDER_NOT_FOUND", "Order " + orderId + " does not exist"));
	}

}
