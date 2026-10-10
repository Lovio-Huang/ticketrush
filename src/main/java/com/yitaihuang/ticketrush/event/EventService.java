package com.yitaihuang.ticketrush.event;

import java.time.Clock;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.yitaihuang.ticketrush.common.AfterCommit;
import com.yitaihuang.ticketrush.reservation.StockGateway;

@Service
public class EventService {

	private final EventRepository events;

	private final EventCatalog catalog;

	private final StockGateway stock;

	private final Clock clock;

	public EventService(EventRepository events, EventCatalog catalog, StockGateway stock, Clock clock) {
		this.events = events;
		this.catalog = catalog;
		this.stock = stock;
		this.clock = clock;
	}

	@Transactional
	public EventResponse create(CreateEventRequest request) {
		Event event = events.save(new Event(request.name(), request.venue(), request.startsAt(),
				request.saleStartsAt(), request.totalTickets(), request.priceCents(), request.perUserLimit(),
				clock.instant()));
		long eventId = event.getId();
		AfterCommit.run(() -> stock.initialize(eventId, request.totalTickets()));
		return EventResponse.of(EventSnapshot.of(event), event.getAvailableTickets());
	}

	@Transactional(readOnly = true)
	public EventResponse get(long eventId) {
		EventSnapshot snapshot = catalog.get(eventId);
		int available = stock.available(eventId)
			.orElseGet(() -> events.findById(eventId).map(Event::getAvailableTickets).orElse(0));
		return EventResponse.of(snapshot, available);
	}

}
