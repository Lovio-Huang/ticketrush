package com.yitaihuang.ticketrush.reservation;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.yitaihuang.ticketrush.config.TicketrushProperties;

/**
 * Hands an accepted reservation to Kafka and reports what is actually known
 * about the outcome. The send is synchronous: normally the client hears
 * "accepted" only after the broker has stored the message (acks=all).
 */
@Component
public class OrderRequestPublisher {

	private static final Logger log = LoggerFactory.getLogger(OrderRequestPublisher.class);

	public enum Delivery {

		/** The broker acknowledged the message. */
		ACKED,

		/** The producer failed before queueing the record, so it was never sent. */
		NOT_SENT,

		/** It may or may not be stored: an ack that timed out or failed after the send. */
		UNKNOWN

	}

	private final KafkaTemplate<String, String> kafka;

	private final String topic;

	private final Duration timeout;

	public OrderRequestPublisher(KafkaTemplate<String, String> kafka, TicketrushProperties properties) {
		this.kafka = kafka;
		this.topic = properties.orders().topic();
		this.timeout = properties.reservations().publishTimeout();
	}

	/**
	 * Keyed by event id: all orders for one event land on the same partition, so
	 * they are written in order by a single consumer with no row contention.
	 */
	public Delivery publish(long eventId, String orderId, String orderJson) {
		CompletableFuture<SendResult<String, String>> ack;
		try {
			// KafkaTemplate throws here only if the producer failed before queueing the
			// record (no metadata within max.block.ms, serialization, producer closed).
			ack = kafka.send(topic, String.valueOf(eventId), orderJson);
		}
		catch (RuntimeException ex) {
			log.warn("Order {} was not sent to Kafka: {}", orderId, ex.toString());
			return Delivery.NOT_SENT;
		}
		try {
			ack.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
			return Delivery.ACKED;
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			return unknown(orderId, ex);
		}
		catch (ExecutionException | TimeoutException ex) {
			// The record left the producer; the broker may have stored it even if the ack never came back.
			return unknown(orderId, ex);
		}
	}

	private static Delivery unknown(String orderId, Exception ex) {
		log.warn("Kafka did not confirm order {}; the outbox relay will retry: {}", orderId, ex.toString());
		return Delivery.UNKNOWN;
	}

}
