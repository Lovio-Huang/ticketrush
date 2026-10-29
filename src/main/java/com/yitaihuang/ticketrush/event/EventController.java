package com.yitaihuang.ticketrush.event;

import java.net.URI;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import org.springframework.data.domain.PageRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.yitaihuang.ticketrush.common.PageResponse;

@RestController
@RequestMapping("/api/events")
public class EventController {

	private final EventService events;

	public EventController(EventService events) {
		this.events = events;
	}

	@PostMapping
	public ResponseEntity<EventResponse> create(@Valid @RequestBody CreateEventRequest request) {
		EventResponse created = events.create(request);
		return ResponseEntity.created(URI.create("/api/events/" + created.id())).body(created);
	}

	@GetMapping
	public PageResponse<EventResponse> upcoming(@RequestParam(defaultValue = "0") @Min(0) int page,
			@RequestParam(defaultValue = "20") @Min(1) @Max(50) int size) {
		return events.upcoming(PageRequest.of(page, size));
	}

	@GetMapping("/{eventId}")
	public EventResponse get(@PathVariable long eventId) {
		return events.get(eventId);
	}

}
