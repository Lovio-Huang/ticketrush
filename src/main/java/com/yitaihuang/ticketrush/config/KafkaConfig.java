package com.yitaihuang.ticketrush.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
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
	 * never going to succeed, so it skips the retries. Redis or MySQL being briefly
	 * unreachable or slow is different: with 2-second command timeouts, 3 quick
	 * retries would dead-letter good orders during a short blip, so those failures
	 * back off exponentially: up to 9 retries, 1, 2, 4, 8, then 10 s apart (65 s of
	 * back-off in all, plus however long each attempt itself takes), each
	 * redelivery well inside max.poll.interval.ms. Offsets are committed only once
	 * a message is stored or dead-lettered, as before.
	 */
	@Bean
	DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
		var recoverer = new DeadLetterPublishingRecoverer(template,
				(record, ex) -> new TopicPartition(deadLetterTopic(record.topic()), -1));
		var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(500L, 3));
		handler.addNotRetryableExceptions(JacksonException.class);
		handler.setBackOffFunction((record, ex) -> isTransient(ex) ? transientBackOff() : null);
		return handler;
	}

	static BackOff transientBackOff() {
		var backOff = new ExponentialBackOff(1_000L, 2.0);
		backOff.setMaxInterval(10_000L);
		backOff.setMaxElapsedTime(60_000L);
		return backOff;
	}

	/** Infrastructure that may well work on the next try, anywhere in the cause chain. */
	static boolean isTransient(Throwable ex) {
		for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
			if (cause instanceof TransientDataAccessException || cause instanceof DataAccessResourceFailureException
					|| cause instanceof RecoverableDataAccessException) {
				return true;
			}
		}
		return false;
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
