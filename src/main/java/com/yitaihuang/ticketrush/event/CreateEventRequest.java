package com.yitaihuang.ticketrush.event;

import java.time.Instant;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Future;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

public record CreateEventRequest(
		@NotBlank @Size(max = 200) String name,
		@NotBlank @Size(max = 200) String venue,
		@NotNull @Future Instant startsAt,
		@NotNull Instant saleStartsAt,
		@Positive @Max(1_000_000) int totalTickets,
		@PositiveOrZero @Max(MAX_PRICE_CENTS) int priceCents,
		@Min(1) @Max(10) int perUserLimit) {

	/**
	 * $1,000,000.00 a ticket: at most 10 tickets an order (the largest quantity
	 * and purchase limit), every amount fits the int amount_cents of orders.
	 */
	public static final int MAX_PRICE_CENTS = 100_000_000;

	@AssertTrue(message = "saleStartsAt must be before startsAt")
	public boolean isSaleWindowValid() {
		return startsAt == null || saleStartsAt == null || saleStartsAt.isBefore(startsAt);
	}

}
