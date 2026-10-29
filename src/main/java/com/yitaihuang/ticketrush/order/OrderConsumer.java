package com.yitaihuang.ticketrush.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import tools.jackson.databind.json.JsonMapper;

@Component
public class OrderConsumer {

	private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

	private final OrderService orders;

	private final JsonMapper json;

	public OrderConsumer(OrderService orders, JsonMapper json) {
		this.orders = orders;
		this.json = json;
	}

	@KafkaListener(topics = "${ticketrush.orders.topic}")
	public void onOrderRequested(String payload) {
		OrderRequested request = json.readValue(payload, OrderRequested.class);
		try {
			orders.recordRequest(request);
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
	}

}
