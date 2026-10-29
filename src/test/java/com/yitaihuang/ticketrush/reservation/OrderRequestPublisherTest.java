package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.config.TicketrushProperties.Orders;
import com.yitaihuang.ticketrush.config.TicketrushProperties.Reservations;
import com.yitaihuang.ticketrush.reservation.OrderRequestPublisher.Delivery;

/** "Definitely not sent" versus "maybe stored": only the first may give tickets back. */
class OrderRequestPublisherTest {

	@SuppressWarnings("unchecked")
	KafkaTemplate<String, String> kafka = mock();

	OrderRequestPublisher publisher = new OrderRequestPublisher(kafka,
			new TicketrushProperties(new Orders("orders", 1, Duration.ofMinutes(10), Duration.ofSeconds(15), 10),
					new Reservations(Duration.ofHours(1), Duration.ofMillis(50), Duration.ofSeconds(10),
							Duration.ofSeconds(30), Duration.ofSeconds(2)),
					null, null, null));

	private Delivery publish() {
		return publisher.publish(1L, "order-1", "{}");
	}

	@Test
	void anAckIsAcked() {
		when(kafka.send(anyString(), anyString(), anyString()))
			.thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

		assertThat(publish()).isEqualTo(Delivery.ACKED);
	}

	@Test
	void aFailureBeforeTheRecordIsQueuedMeansItWasNotSent() {
		// KafkaTemplate throws from send() itself, e.g. no metadata within max.block.ms.
		when(kafka.send(anyString(), anyString(), anyString())).thenThrow(new KafkaException("Send failed"));

		assertThat(publish()).isEqualTo(Delivery.NOT_SENT);
	}

	@Test
	void aFailedAckMightStillHaveBeenStored() {
		when(kafka.send(anyString(), anyString(), anyString()))
			.thenReturn(CompletableFuture.failedFuture(new KafkaException("NotEnoughReplicasAfterAppend")));

		assertThat(publish()).isEqualTo(Delivery.UNKNOWN);
	}

	@Test
	void anAckThatNeverArrivesIsUnknown() {
		when(kafka.send(anyString(), anyString(), anyString())).thenReturn(new CompletableFuture<>());

		assertThat(publish()).isEqualTo(Delivery.UNKNOWN);
	}

}
