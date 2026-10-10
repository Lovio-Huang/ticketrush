package com.yitaihuang.ticketrush.event;

import java.net.URI;

import jakarta.validation.Valid;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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

	@GetMapping("/{eventId}")
	public EventResponse get(@PathVariable long eventId) {
		return events.get(eventId);
	}

}
