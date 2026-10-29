package com.yitaihuang.ticketrush.order;

import java.util.EnumMap;
import java.util.Map;

import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.yitaihuang.ticketrush.common.ApiException;
import com.yitaihuang.ticketrush.event.Event;
import com.yitaihuang.ticketrush.event.EventRepository;
import com.yitaihuang.ticketrush.reservation.StockGateway;

/** Admin view of one event's sale: where every ticket is, and whether Redis agrees with MySQL. */
@RestController
public class SalesStatsController {

	private final EventRepository events;

	private final OrderRepository orders;

	private final StockGateway stock;

	public SalesStatsController(EventRepository events, OrderRepository orders, StockGateway stock) {
		this.events = events;
		this.orders = orders;
		this.stock = stock;
	}

	public record Totals(long orders, long tickets, long amountCents) {
	}

	public record SalesStats(long eventId, String name, int totalTickets, int availableInDatabase,
			Integer availableInRedis, boolean storesInSync, long ticketsSold, long revenueCents,
			Map<OrderStatus, Totals> byStatus) {
	}

	@GetMapping("/api/events/{eventId}/stats")
	@Transactional(readOnly = true)
	public SalesStats stats(@PathVariable long eventId) {
		Event event = events.findById(eventId)
			.orElseThrow(() -> ApiException.notFound("EVENT_NOT_FOUND", "Event " + eventId + " does not exist"));

		Map<OrderStatus, Totals> byStatus = new EnumMap<>(OrderStatus.class);
		for (OrderStatus status : OrderStatus.values()) {
			byStatus.put(status, new Totals(0, 0, 0));
		}
		for (OrderRepository.StatusTotals row : orders.totalsByStatus(eventId)) {
			byStatus.put(row.getStatus(), new Totals(row.getOrders(), row.getTickets(), row.getAmountCents()));
		}

		var redisStock = stock.available(eventId);
		Integer inRedis = redisStock.isPresent() ? redisStock.getAsInt() : null;
		Totals paid = byStatus.get(OrderStatus.PAID);
		return new SalesStats(eventId, event.getName(), event.getTotalTickets(), event.getAvailableTickets(), inRedis,
				inRedis != null && inRedis == event.getAvailableTickets(), paid.tickets(), paid.amountCents(),
				byStatus);
	}

}
