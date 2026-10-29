package com.yitaihuang.ticketrush.order;

public enum OrderStatus {

	/** Tickets are held; the customer must pay before {@code expiresAt}. */
	PENDING_PAYMENT,

	PAID,

	/** Not paid in time; tickets went back on sale. */
	EXPIRED,

	CANCELLED,

	/** Redis accepted it but MySQL had no stock left (Redis was out of sync). */
	REJECTED;

	/** Whether an order in this state still owns its tickets. */
	public boolean holdsTickets() {
		return this == PENDING_PAYMENT || this == PAID;
	}

}
