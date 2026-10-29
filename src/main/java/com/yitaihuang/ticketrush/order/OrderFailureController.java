package com.yitaihuang.ticketrush.order;

import java.time.Instant;
import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Admin-only handling of dead-lettered orders (see {@link OrderFailureService}). */
@RestController
@RequestMapping("/api/admin/order-failures")
public class OrderFailureController {

	private final OrderFailureService failures;

	public OrderFailureController(OrderFailureService failures) {
		this.failures = failures;
	}

	public record Resolution(String note) {
	}

	public record FailureResponse(long id, String orderId, Long eventId, String status, int attempts, String error,
			String dltPosition, String payload, String note, Instant firstFailedAt, Instant lastFailedAt,
			Instant resolvedAt) {

		static FailureResponse of(OrderFailure f) {
			return new FailureResponse(f.getId(), f.getOrderId(), f.getEventId(), f.getStatus().name(),
					f.getAttempts(), f.getError(), f.getDltPosition(), f.getPayload(), f.getNote(),
					f.getFirstFailedAt(), f.getLastFailedAt(), f.getResolvedAt());
		}

	}

	@GetMapping
	public List<FailureResponse> list(@RequestParam(required = false) OrderFailure.Status status) {
		return failures.list(status).stream().map(FailureResponse::of).toList();
	}

	@PostMapping("/{id}/replay")
	public FailureResponse replay(@PathVariable long id, @RequestBody(required = false) Resolution body) {
		return FailureResponse.of(failures.replay(id, note(body)));
	}

	@PostMapping("/{id}/void")
	public FailureResponse voidOrder(@PathVariable long id, @RequestBody(required = false) Resolution body) {
		return FailureResponse.of(failures.voidOrder(id, note(body)));
	}

	@PostMapping("/{id}/discard")
	public FailureResponse discard(@PathVariable long id, @RequestBody(required = false) Resolution body) {
		return FailureResponse.of(failures.discard(id, note(body)));
	}

	private static String note(Resolution body) {
		return body == null ? null : body.note();
	}

}
