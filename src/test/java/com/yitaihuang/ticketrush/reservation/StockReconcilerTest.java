package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.OptionalLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.yitaihuang.ticketrush.event.EventRepository;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/** When the reconciler trusts a mismatch enough to fix it, and when it holds back. */
class StockReconcilerTest {

	EventRepository events = mock(EventRepository.class);

	StockGateway stock = mock(StockGateway.class);

	OrderQueueLag lag = mock(OrderQueueLag.class);

	SimpleMeterRegistry meters = new SimpleMeterRegistry();

	StockReconciler reconciler;

	@BeforeEach
	void setUp() {
		reconciler = new StockReconciler(events, stock, lag, meters,
				Clock.fixed(Instant.parse("2026-10-28T12:00:00Z"), ZoneOffset.UTC));
		when(stock.compareAndSet(anyLong(), anyInt(), anyInt())).thenReturn(true);
	}

	private double corrections() {
		return meters.counter("ticketrush.stock.corrections").count();
	}

	@Test
	void aMismatchSeenOnceIsOnlySuspected() {
		reconciler.check(1, 10, 7);

		verify(stock, never()).compareAndSet(anyLong(), anyInt(), anyInt());
	}

	@Test
	void theSameMismatchTwiceInARowIsFixedByCompareAndSet() {
		reconciler.check(1, 10, 7);
		reconciler.check(1, 10, 7);

		verify(stock).compareAndSet(1, 7, 10);
		assertThat(corrections()).isEqualTo(1);
	}

	@Test
	void numbersThatKeepMovingAreLeftAlone() {
		// A sale in progress: both stores change between runs, so the gap isn't settled drift.
		reconciler.check(1, 10, 7);
		reconciler.check(1, 9, 6);
		reconciler.check(1, 8, 5);

		verify(stock, never()).compareAndSet(anyLong(), anyInt(), anyInt());
	}

	@Test
	void agreeingOnceClearsTheSuspicion() {
		reconciler.check(1, 10, 7);
		reconciler.check(1, 10, 10);
		reconciler.check(1, 10, 7);

		verify(stock, never()).compareAndSet(anyLong(), anyInt(), anyInt());
	}

	@Test
	void aLostCompareAndSetIsNotCounted() {
		// A reservation landed between the check and the fix; it must not be overwritten.
		when(stock.compareAndSet(1, 7, 10)).thenReturn(false);
		reconciler.check(1, 10, 7);
		reconciler.check(1, 10, 7);

		assertThat(corrections()).isZero();
	}

	@Test
	void aMissingRedisKeyIsSeededFromMysql() {
		reconciler.check(1, 10, null);

		verify(stock).initializeIfAbsent(1, 10);
	}

	@Test
	void ordersStillInKafkaOrAnUnknownLagMeanNoChecksAtAll() {
		when(lag.current()).thenReturn(OptionalLong.of(3));
		reconciler.run();
		when(lag.current()).thenReturn(OptionalLong.empty());
		reconciler.run();

		verifyNoInteractions(events);
	}

}
