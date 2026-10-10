package com.yitaihuang.ticketrush.reservation;

import java.net.URI;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code X-User-Id} stands in for an authenticated principal; in production it
 * would come from a validated JWT set by the API gateway.
 */
@RestController
public class ReservationController {

	private final ReservationService reservations;

	public ReservationController(ReservationService reservations) {
		this.reservations = reservations;
	}

	public record ReservationRequest(@Min(1) @Max(10) int quantity) {
	}

	public record ReservationResponse(String orderId, String statusUrl) {
	}

	@PostMapping("/api/events/{eventId}/reservations")
	public ResponseEntity<ReservationResponse> reserve(@PathVariable long eventId,
			@RequestHeader("X-User-Id") @NotBlank @Size(max = 64) String userId,
			@RequestHeader("Idempotency-Key") @NotBlank @Size(max = 64) String idempotencyKey,
			@Valid @RequestBody ReservationRequest request) {
		ReservationService.Result result = reservations.reserve(eventId, userId, request.quantity(), idempotencyKey);
		String statusUrl = "/api/orders/" + result.orderId();
		return ResponseEntity.accepted()
			.location(URI.create(statusUrl))
			.header("Idempotent-Replayed", String.valueOf(result.replayed()))
			.body(new ReservationResponse(result.orderId(), statusUrl));
	}

}
