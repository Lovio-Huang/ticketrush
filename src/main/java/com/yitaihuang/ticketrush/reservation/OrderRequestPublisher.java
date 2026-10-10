package com.yitaihuang.ticketrush.reservation;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.order.OrderRequested;

import tools.jackson.databind.json.JsonMapper;

/**
 * Hands an accepted reservation to Kafka. The send is synchronous: we only tell
 * the client "accepted" after the broker has durably stored the message
 * (acks=all), trading a few milliseconds of latency for never losing an order.
 */
@Component
public class OrderRequestPublisher {

	private final KafkaTemplate<String, String> kafka;

	private final JsonMapper json;

	private final String topic;

	private final Duration timeout;

	public OrderRequestPublisher(KafkaTemplate<String, String> kafka, JsonMapper json,
			TicketrushProperties properties) {
		this.kafka = kafka;
		this.json = json;
		this.topic = properties.orders().topic();
		this.timeout = properties.reservations().publishTimeout();
	}

	public void publish(OrderRequested request) {
		// Keyed by event id: all orders for one event land on the same partition,
		// so they are written in order by a single consumer with no row contention.
		String key = String.valueOf(request.eventId());
		try {
			kafka.send(topic, key, json.writeValueAsString(request)).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new OrderPublishException(request.orderId(), ex);
		}
		catch (ExecutionException | TimeoutException ex) {
			throw new OrderPublishException(request.orderId(), ex);
		}
	}

	public static class OrderPublishException extends RuntimeException {

		OrderPublishException(String orderId, Throwable cause) {
			super("Kafka did not acknowledge order " + orderId, cause);
		}

	}

}
