package com.yitaihuang.ticketrush.reservation;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import com.yitaihuang.ticketrush.config.TicketrushProperties;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * How many accepted orders are still waiting in Kafka for the order consumer.
 * While this is non-zero, Redis is legitimately ahead of MySQL.
 */
@Component
public class OrderQueueLag implements DisposableBean {

	private static final Logger log = LoggerFactory.getLogger(OrderQueueLag.class);

	private static final long TIMEOUT_SECONDS = 5;

	private final AdminClient admin;

	private final String topic;

	private final String groupId;

	public OrderQueueLag(KafkaAdmin kafkaAdmin, TicketrushProperties properties,
			@Value("${spring.kafka.consumer.group-id}") String groupId, MeterRegistry meters) {
		this.admin = AdminClient.create(kafkaAdmin.getConfigurationProperties());
		this.topic = properties.orders().topic();
		this.groupId = groupId;
		// Read on each scrape: accepted orders not yet written to MySQL (-1 if Kafka can't be asked).
		Gauge.builder("ticketrush.orders.queue.lag", this, lag -> lag.current().orElse(-1)).register(meters);
	}

	/** Unprocessed messages across all partitions, or empty if Kafka couldn't be asked. */
	public OptionalLong current() {
		try {
			Map<TopicPartition, OffsetSpec> latestRequest = admin.describeTopics(List.of(topic))
				.allTopicNames()
				.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
				.get(topic)
				.partitions()
				.stream()
				.collect(Collectors.toMap(p -> new TopicPartition(topic, p.partition()), p -> OffsetSpec.latest()));
			Map<TopicPartition, ListOffsetsResultInfo> latest = admin.listOffsets(latestRequest)
				.all()
				.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
			Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
				.partitionsToOffsetAndMetadata()
				.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

			long lag = 0;
			for (var entry : latest.entrySet()) {
				OffsetAndMetadata done = committed.get(entry.getKey());
				lag += entry.getValue().offset() - (done == null ? 0 : done.offset());
			}
			return OptionalLong.of(lag);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			return OptionalLong.empty();
		}
		catch (ExecutionException | TimeoutException ex) {
			log.warn("Could not read consumer lag for {}: {}", topic, ex.getMessage());
			return OptionalLong.empty();
		}
	}

	@Override
	public void destroy() {
		admin.close();
	}

}
