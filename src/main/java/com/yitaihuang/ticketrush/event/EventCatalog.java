package com.yitaihuang.ticketrush.event;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import com.yitaihuang.ticketrush.common.ApiException;

/**
 * Read-through cache for event metadata. Every reservation needs the sale start
 * time, price and per-user limit, so this keeps MySQL off the hot path.
 */
@Component
public class EventCatalog {

	private final EventRepository events;

	public EventCatalog(EventRepository events) {
		this.events = events;
	}

	/**
	 * {@code sync = true}: when a sale opens, hundreds of requests miss the cache
	 * at the same instant. Only one of them loads from MySQL; the rest wait for
	 * that result instead of stampeding the database.
	 */
	@Cacheable(cacheNames = "events", key = "#eventId", sync = true)
	public EventSnapshot get(long eventId) {
		return events.findById(eventId)
			.map(EventSnapshot::of)
			.orElseThrow(() -> ApiException.notFound("EVENT_NOT_FOUND", "Event " + eventId + " does not exist"));
	}

}
