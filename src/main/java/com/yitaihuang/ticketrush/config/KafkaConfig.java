package com.yitaihuang.ticketrush.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import tools.jackson.core.JacksonException;

@Configuration(proxyBeanMethods = false)
public class KafkaConfig {

	@Bean
	NewTopic ordersTopic(TicketrushProperties properties) {
		return TopicBuilder.name(properties.orders().topic()).partitions(properties.orders().partitions()).build();
	}

	@Bean
	NewTopic ordersDeadLetterTopic(TicketrushProperties properties) {
		return TopicBuilder.name(deadLetterTopic(properties.orders().topic())).partitions(1).build();
	}

	/**
	 * Retries a failing message 3 times, then parks it on the dead-letter topic so
	 * one poison message cannot block its partition forever. Malformed JSON is
	 * never going to succeed, so it skips the retries.
	 */
	@Bean
	DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
		var recoverer = new DeadLetterPublishingRecoverer(template,
				(record, ex) -> new TopicPartition(deadLetterTopic(record.topic()), -1));
		var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(500L, 3));
		handler.addNotRetryableExceptions(JacksonException.class);
		return handler;
	}

	/**
	 * For the dead-letter listener: retries forever instead of dead-lettering the
	 * dead letters. A failure record must not be lost, and the listener only
	 * fails when MySQL is unavailable.
	 */
	@Bean
	ConcurrentKafkaListenerContainerFactory<String, String> deadLetterContainerFactory(
			ConsumerFactory<String, String> consumerFactory) {
		var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
		factory.setConsumerFactory(consumerFactory);
		factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(2000L, FixedBackOff.UNLIMITED_ATTEMPTS)));
		return factory;
	}

	static String deadLetterTopic(String topic) {
		return topic + ".DLT";
	}

}
