package com.yitaihuang.ticketrush.order;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.yitaihuang.ticketrush.common.AfterCommit;
import com.yitaihuang.ticketrush.common.ApiException;
import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.event.EventRepository;
import com.yitaihuang.ticketrush.reservation.StockGateway;

@Service
public class OrderService {

	private static final Logger log = LoggerFactory.getLogger(OrderService.class);

	private final OrderRepository orders;

	private final EventRepository events;

	private final StockGateway stock;

	private final TicketrushProperties properties;

	private final Clock clock;

	public OrderService(OrderRepository orders, EventRepository events, StockGateway stock,
			TicketrushProperties properties, Clock clock) {
		this.orders = orders;
		this.events = events;
		this.stock = stock;
		this.properties = properties;
		this.clock = clock;
	}

	/**
	 * Persists an order handed over by Kafka. Kafka delivers at least once, so
	 * this must be idempotent: a redelivered message finds the existing row and
	 * does nothing (the primary key catches the rare concurrent duplicate).
	 */
	@Transactional
	public void recordRequest(OrderRequested request) {
		if (orders.existsById(request.orderId())) {
			return;
		}
		Instant now = clock.instant();
		if (events.decrementAvailable(request.eventId(), request.quantity()) == 0) {
			// Redis said yes but MySQL has no stock left: Redis was out of sync
			// (e.g. reloaded while orders were in flight). MySQL wins.
			log.warn("Rejecting order {}: event {} has no stock in MySQL", request.orderId(), request.eventId());
			orders.save(Order.rejected(request, now));
			AfterCommit.run(() -> stock.releaseBuyerHold(request.eventId(), request.userId(), request.quantity()));
			return;
		}
		orders.save(Order.pendingPayment(request, now, properties.orders().paymentWindow()));
	}

	@Transactional(readOnly = true)
	public Optional<Order> find(String orderId, String userId) {
		// Someone else's order looks exactly like a missing one: no ID probing.
		return orders.findById(orderId).filter(order -> order.isOwnedBy(userId));
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
		AfterCommit.run(() -> stock.release(order.getEventId(), order.getUserId(), order.getQuantity()));
	}

	private Order require(String orderId, String userId) {
		return find(orderId, userId)
			.orElseThrow(() -> ApiException.notFound("ORDER_NOT_FOUND", "Order " + orderId + " does not exist"));
	}

}
