package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.yitaihuang.ticketrush.AbstractIntegrationTest;

import io.micrometer.core.instrument.MeterRegistry;

/** Several instances noticing the same Redis restart: exactly one wipe, and nothing after the rebuild is lost. */
class RedisRestartGuardIntegrationTest extends AbstractIntegrationTest {

	@Autowired StockGateway stock;

	@Autowired StockRecovery recovery;

	@Autowired MeterRegistry meters;

	@Autowired RedisRestartGuard guard;

	@Test
	void anInstanceThatNoticedTheRestartLateDoesNotWipeTheRebuildAnotherOneFinished() throws Exception {
		long eventId = createEvent(10, 4);
		String user = String.valueOf(userId("late-guard"));
		reserveAndAwaitOrder(eventId, user, 2);
		redis.opsForValue().set(RedisRestartGuard.SERVER_KEY, "the-server-before-the-restart");

		// Instance B learns the server's run_id, then stalls before acting on it.
		CountDownLatch bLooked = new CountDownLatch(1);
		CountDownLatch aDone = new CountDownLatch(1);
		StockGateway stockB = spy(stock);
		doAnswer(call -> {
			Object seen = call.callRealMethod();
			bLooked.countDown();
			awaitLatch(aDone);
			return seen;
		}).when(stockB).serverRunId();
		RedisRestartGuard a = new RedisRestartGuard(stock, meters);
		RedisRestartGuard b = new RedisRestartGuard(stockB, meters);

		try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
			Future<?> bChecking = pool.submit(b::check);
			awaitLatch(bLooked);
			// Instance A handles the restart; the event is rebuilt and sells again.
			a.check();
			quietPeriodOver(eventId);
			assertThat(recovery.rebuild(eventId)).isTrue();
			reserveAndAwaitOrder(eventId, user, 1);
			aDone.countDown();
			bChecking.get();
		}

		assertThat(stock.available(eventId)).hasValue(7);
		assertStoresAgree(eventId);
	}

	@Test
	void aProcessStartingOnARestoredSnapshotTrustsNothingUntilItsCheckHasWipedIt() {
		long eventId = createEvent(10, 4);
		reserveAndAwaitOrder(eventId, String.valueOf(userId("snapshot")), 2);
		// All app instances were down; Redis came back from a snapshot that still carries
		// the record of the server it was taken on.
		redis.opsForValue().set(RedisRestartGuard.SERVER_KEY, "the-server-the-snapshot-was-taken-on");

		RedisRestartGuard starting = new RedisRestartGuard(stock, meters);
		assertThat(starting.verified()).isFalse();
		starting.check();

		assertThat(starting.verified()).isTrue();
		assertThat(stock.available(eventId)).isEmpty(); // wiped: the event is rebuilt from MySQL
	}

	@Test
	void whileAWipeIsUnfinishedNoInstanceSellsOrRebuilds() {
		long eventId = createEvent(10, 4);
		String user = String.valueOf(userId("dead-wiper"));
		reserveAndAwaitOrder(eventId, user, 2);
		// An instance noticed a restart, deleted this event's keys, and died before finishing.
		redis.opsForValue().set(RedisRestartGuard.SERVER_KEY, "the-server-before-the-restart");
		redis.opsForValue().set(RedisRestartGuard.WIPE_LOCK, "a-wiper-that-died");
		wipeRedis(eventId);
		quietPeriodOver(eventId);

		guard.check();
		assertThat(guard.verified()).isFalse();
		assertThat(recovery.rebuild(eventId)).isFalse();
		assertThatThrownBy(() -> reservations.reserve(eventId, user, 1, "k2"))
			.hasFieldOrPropertyWithValue("code", "STOCK_RECOVERING");

		redis.delete(RedisRestartGuard.WIPE_LOCK); // its lease runs out; the next check redoes the wipe
		guard.check();
		assertThat(guard.verified()).isTrue();
		quietPeriodOver(eventId);
		assertThat(recovery.rebuild(eventId)).isTrue();
		assertStoresAgree(eventId);
	}

	private static void awaitLatch(CountDownLatch latch) {
		try {
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(ex);
		}
	}

}
