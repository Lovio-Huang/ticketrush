package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.yitaihuang.ticketrush.AbstractIntegrationTest;

import io.micrometer.core.instrument.MeterRegistry;

class StockReconcilerIntegrationTest extends AbstractIntegrationTest {

	@Autowired
	StockReconciler reconciler;

	@Autowired
	StockGateway stock;

	@Autowired
	OrderQueueLag lag;

	@Autowired
	MeterRegistry meters;

	@Test
	void redisDriftIsCorrectedToMysqlOnTheSecondRun() {
		long eventId = createEvent(10, 4);
		reserveAndAwaitOrder(eventId, String.valueOf(userId("mia")), 2); // both stores at 8
		await().atMost(Duration.ofSeconds(15)).until(() -> lag.current().orElse(-1) == 0);
		double before = meters.counter("ticketrush.stock.corrections").count();

		stock.initialize(eventId, 5); // e.g. a crash between an expiry commit and the Redis release
		reconciler.run();
		assertThat(stock.available(eventId)).hasValue(5); // first sighting: only suspected
		reconciler.run();

		assertThat(stock.available(eventId)).hasValue(8);
		assertThat(meters.counter("ticketrush.stock.corrections").count()).isGreaterThan(before);
	}

}
