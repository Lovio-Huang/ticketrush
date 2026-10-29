package com.yitaihuang.ticketrush.order;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import tools.jackson.databind.json.JsonMapper;

@Component
public class OrderConsumer {

	private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

	private final OrderService orders;

	private final JsonMapper json;

	/** One MySQL transaction (a batch, or one message), Redis claims and commit included. */
	private final Timer writes;

	private final Counter consumed;

	public OrderConsumer(OrderService orders, JsonMapper json, MeterRegistry meters) {
		this.orders = orders;
		this.json = json;
		this.writes = Timer.builder("ticketrush.orders.write").publishPercentiles(0.5, 0.95, 0.99).register(meters);
		this.consumed = meters.counter("ticketrush.orders.consumed");
	}

	/**
	 * One poll's worth of messages in one transaction. If the batch fails it has
	 * rolled back as a whole (the Redis claims it made are idempotent and stay),
	 * and the messages are redone one by one, each in its own transaction. The
	 * first one that fails again is reported by its index: the error handler
	 * commits the offsets before it (those orders are stored), retries from it,
	 * and dead-letters only that message, so a good order is never dropped or
	 * rejected because of its neighbour.
	 */
	@KafkaListener(id = "order-writer", idIsGroup = false, topics = "${ticketrush.orders.topic}", batch = "true")
	public void onOrderRequests(List<String> payloads) {
		try {
			List<OrderRequested> requests = payloads.stream()
				.map(payload -> json.readValue(payload, OrderRequested.class))
				.toList();
			writes.record(() -> orders.recordRequests(requests));
			consumed.increment(payloads.size());
			return;
		}
		catch (RuntimeException ex) {
			log.info("Batch of {} order messages failed ({}); retrying them one by one", payloads.size(), ex.toString());
		}
		for (int i = 0; i < payloads.size(); i++) {
			try {
				onOrderRequested(payloads.get(i));
			}
			catch (RuntimeException ex) {
				// The dead letter's error header carries this message: keep the real cause readable in it.
				throw new BatchListenerFailedException("Order message " + i + " of the batch failed: " + ex, ex, i);
			}
		}
	}

	public void onOrderRequested(String payload) {
		OrderRequested request = json.readValue(payload, OrderRequested.class);
		try {
			writes.record(() -> orders.recordRequest(request));
		}
		catch (DataIntegrityViolationException ex) {
			// Skip only if this very order is stored (another consumer inserted it a
			// moment ago, e.g. during a rebalance). Any other constraint error goes
			// through the error handler: retries, then the dead-letter topic.
			if (!orders.acknowledgeReplay(request)) {
				throw ex;
			}
			log.info("Order {} was already recorded, skipping redelivery", request.orderId());
		}
		consumed.increment();
	}

}
