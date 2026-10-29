package com.yitaihuang.ticketrush.order;

import java.time.Instant;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/** A dead-lettered order message and what an operator decided about it. */
@Entity
@Table(name = "order_failures")
public class OrderFailure {

	public enum Status {

		/** Waiting for a decision; if it is an order, its tickets are still held in Redis. */
		OPEN,

		/** Sent to the order topic again; reopens if it fails again. */
		REPLAYED,

		/** Given up: recorded as a rejected order and its tickets released. Final. */
		VOIDED,

		/** Acknowledged without touching stock (unparseable, or the order already exists). Final. */
		DISCARDED

	}

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	private String orderId;

	private Long eventId;

	private String payload;

	private String error;

	private String dltPosition;

	private String lastDltPosition;

	private int attempts;

	@Enumerated(EnumType.STRING)
	@JdbcTypeCode(SqlTypes.VARCHAR)
	private Status status;

	private String note;

	private Instant firstFailedAt;

	private Instant lastFailedAt;

	private Instant resolvedAt;

	@Version
	private Long version;

	protected OrderFailure() {
	}

	OrderFailure(OrderRequested request, String payload, String error, String dltPosition, Instant now) {
		this.orderId = request == null ? null : request.orderId();
		this.eventId = request == null ? null : request.eventId();
		this.payload = payload;
		this.error = truncate(error, 1000);
		this.dltPosition = dltPosition;
		this.lastDltPosition = dltPosition;
		this.attempts = 1;
		this.status = Status.OPEN;
		this.firstFailedAt = now;
		this.lastFailedAt = now;
	}

	/**
	 * The same order was dead-lettered again. A replayed one reopens; a final decision stands.
	 * shortcut: only the first and the latest DLT record are remembered, so redelivering an older
	 * repeat counts an extra attempt; it never changes a final decision.
	 */
	void failedAgain(String error, String dltPosition, Instant now) {
		this.attempts++;
		this.lastDltPosition = dltPosition;
		this.error = truncate(error, 1000);
		this.lastFailedAt = now;
		if (status == Status.REPLAYED) {
			status = Status.OPEN;
		}
	}

	void resolve(Status outcome, String note, Instant now) {
		this.status = outcome;
		this.note = truncate(note, 500);
		this.resolvedAt = outcome == Status.REPLAYED ? null : now;
	}

	boolean isFinal() {
		return status == Status.VOIDED || status == Status.DISCARDED;
	}

	private static String truncate(String value, int max) {
		return value == null || value.length() <= max ? value : value.substring(0, max);
	}

	public Long getId() {
		return id;
	}

	public String getOrderId() {
		return orderId;
	}

	public Long getEventId() {
		return eventId;
	}

	public String getPayload() {
		return payload;
	}

	public String getError() {
		return error;
	}

	public String getDltPosition() {
		return dltPosition;
	}

	public int getAttempts() {
		return attempts;
	}

	public Status getStatus() {
		return status;
	}

	public String getNote() {
		return note;
	}

	public Instant getFirstFailedAt() {
		return firstFailedAt;
	}

	public Instant getLastFailedAt() {
		return lastFailedAt;
	}

	public Instant getResolvedAt() {
		return resolvedAt;
	}

}
