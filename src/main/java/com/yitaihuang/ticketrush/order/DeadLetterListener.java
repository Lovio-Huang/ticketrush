package com.yitaihuang.ticketrush.order;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

/**
 * Turns each dead-lettered order message into an {@link OrderFailure} record,
 * with the exception the consumer gave up on. Runs with its own error handler
 * that retries forever: losing a failure record would hide held tickets.
 */
@Component
public class DeadLetterListener {

	private final OrderFailureService failures;

	public DeadLetterListener(OrderFailureService failures) {
		this.failures = failures;
	}

	@KafkaListener(id = "order-failures", topics = "${ticketrush.orders.topic}.DLT", groupId = "order-failures",
			containerFactory = "deadLetterContainerFactory")
	public void onDeadLetter(ConsumerRecord<String, String> record) {
		String error = header(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN) + " (in "
				+ header(record, KafkaHeaders.DLT_EXCEPTION_FQCN) + "): " + header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
		String position = record.topic() + "-" + record.partition() + "@" + record.offset();
		failures.record(record.value() == null ? "" : record.value(), error, position);
	}

	private static String header(ConsumerRecord<?, ?> record, String name) {
		Header header = record.headers().lastHeader(name);
		return header == null ? "?" : new String(header.value(), StandardCharsets.UTF_8);
	}

}
