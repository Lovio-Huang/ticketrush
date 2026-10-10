package com.yitaihuang.ticketrush.event;

import java.time.Instant;

public record EventResponse(long id, String name, String venue, Instant startsAt, Instant saleStartsAt,
		int totalTickets, int availableTickets, int priceCents, int perUserLimit) {

	static EventResponse of(EventSnapshot event, int availableTickets) {
		return new EventResponse(event.id(), event.name(), event.venue(), event.startsAt(), event.saleStartsAt(),
				event.totalTickets(), availableTickets, event.priceCents(), event.perUserLimit());
	}

}
