package com.yitaihuang.ticketrush.event;

import java.time.Instant;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "events")
public class Event {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	private String name;

	private String venue;

	private Instant startsAt;

	private Instant saleStartsAt;

	private int totalTickets;

	/** Source of truth for inventory. Redis holds a fast-moving copy of it. */
	private int availableTickets;

	private int priceCents;

	private int perUserLimit;

	private Instant createdAt;

	/** Set once the sale is over and no accepted order needs the background jobs any more. */
	private Instant settledAt;

	/** Bumped by every write that may need the background jobs later (see EventRepository#touch). */
	private long upkeepVersion;

	protected Event() {
	}

	public Event(String name, String venue, Instant startsAt, Instant saleStartsAt, int totalTickets, int priceCents,
			int perUserLimit, Instant createdAt) {
		this.name = name;
		this.venue = venue;
		this.startsAt = startsAt;
		this.saleStartsAt = saleStartsAt;
		this.totalTickets = totalTickets;
		this.availableTickets = totalTickets;
		this.priceCents = priceCents;
		this.perUserLimit = perUserLimit;
		this.createdAt = createdAt;
	}

	public Long getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getVenue() {
		return venue;
	}

	public Instant getStartsAt() {
		return startsAt;
	}

	public Instant getSaleStartsAt() {
		return saleStartsAt;
	}

	public int getTotalTickets() {
		return totalTickets;
	}

	public int getAvailableTickets() {
		return availableTickets;
	}

	public int getPriceCents() {
		return priceCents;
	}

	public int getPerUserLimit() {
		return perUserLimit;
	}

	public Instant getCreatedAt() {
		return createdAt;
	}

	public Instant getSettledAt() {
		return settledAt;
	}

	public long getUpkeepVersion() {
		return upkeepVersion;
	}

}
