package com.yitaihuang.ticketrush.event;

import java.time.Instant;

/**
 * The parts of an event that never change after creation, which makes them
 * safe to cache in memory on every instance without invalidation.
 */
public record EventSnapshot(long id, String name, String venue, Instant startsAt, Instant saleStartsAt,
		int totalTickets, int priceCents, int perUserLimit) {

	static EventSnapshot of(Event event) {
		return new EventSnapshot(event.getId(), event.getName(), event.getVenue(), event.getStartsAt(),
				event.getSaleStartsAt(), event.getTotalTickets(), event.getPriceCents(), event.getPerUserLimit());
	}

}
