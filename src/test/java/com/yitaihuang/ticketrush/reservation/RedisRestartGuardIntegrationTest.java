package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.yitaihuang.ticketrush.AbstractIntegrationTest;
import com.yitaihuang.ticketrush.reservation.StockGateway.RestartCheck;

import io.micrometer.core.instrument.MeterRegistry;

/** Several instances noticing the same Redis restart: exactly one wipe, and nothing after the rebuild is lost. */
class RedisRestartGuardIntegrationTest extends AbstractIntegrationTest {

	@Autowired StockGateway stock;

	@Autowired StockRecovery recovery;

	@Autowired MeterRegistry meters;

	@Autowired RedisRestartGuard guard;

	/** A test that failed halfway must not leave the next one a held lock or an untrusted server. */
	@AfterEach
	void trustTheServerAgain() {
		redis.delete(RedisRestartGuard.WIPE_LOCK);
		redis.opsForValue().set(RedisRestartGuard.SERVER_KEY, stock.serverRunId());
	}

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

	@Test
	void aWiperThatStalledPastItsLeaseCannotDeleteTheStateItsSuccessorRebuilt() throws Exception {
		long eventId = createEvent(10, 4);
		String user = String.valueOf(userId("stalled-wiper"));
		reserveAndAwaitOrder(eventId, user, 2);
		redis.opsForValue().set(RedisRestartGuard.SERVER_KEY, "the-server-before-the-restart");

		// Instance A takes the wipe lock, then stalls (a long GC pause) before it deletes anything.
		CountDownLatch aStalled = new CountDownLatch(1);
		CountDownLatch aWakes = new CountDownLatch(1);
		StockGateway stockA = spy(stock);
		doAnswer(call -> {
			aStalled.countDown();
			awaitLatch(aWakes);
			return call.callRealMethod();
		}).when(stockA).wipeAllEvents(anyString(), any());
		RedisRestartGuard a = new RedisRestartGuard(stockA, meters);
		RedisRestartGuard b = new RedisRestartGuard(stock, meters);

		try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
			Future<?> aChecking = pool.submit(a::check);
			awaitLatch(aStalled);
			redis.delete(RedisRestartGuard.WIPE_LOCK); // A's lease runs out while it is stalled
			// Instance B takes over, wipes and trusts the server; the event is rebuilt and sells again.
			b.check();
			assertThat(b.verified()).isTrue();
			quietPeriodOver(eventId);
			assertThat(recovery.rebuild(eventId)).isTrue();
			reserveAndAwaitOrder(eventId, user, 1);
			aWakes.countDown();
			aChecking.get();
		}

		assertThat(stock.available(eventId)).hasValue(7);
		assertStoresAgree(eventId);
		assertThat(a.verified()).isFalse(); // A never announces a wipe it no longer owned
	}

	@Test
	void aWiperThatStalledMidWipeDeletesNoneOfTheKeysItHadAlreadyListed() throws Exception {
		long eventId = createEvent(10, 4);
		String user = String.valueOf(userId("mid-wipe"));
		reserveAndAwaitOrder(eventId, user, 2);
		redis.opsForValue().set(RedisRestartGuard.SERVER_KEY, "the-server-before-the-restart");

		// A has scanned the stock keys and stalls right before deleting that batch.
		CountDownLatch aStalled = new CountDownLatch(1);
		CountDownLatch aWakes = new CountDownLatch(1);
		StockGateway stockA = spy(stock);
		doAnswer(call -> {
			if (aStalled.getCount() == 1 && ((List<?>) call.getArgument(0)).size() > 0) {
				aStalled.countDown();
				awaitLatch(aWakes);
			}
			return call.callRealMethod();
		}).when(stockA).unlinkOwned(anyList(), anyString(), any());
		RedisRestartGuard a = new RedisRestartGuard(stockA, meters);

		try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
			Future<?> aChecking = pool.submit(a::check);
			awaitLatch(aStalled);
			redis.delete(RedisRestartGuard.WIPE_LOCK); // A's lease runs out
			new RedisRestartGuard(stock, meters).check(); // B wipes and trusts the server
			quietPeriodOver(eventId);
			assertThat(recovery.rebuild(eventId)).isTrue();
			reserveAndAwaitOrder(eventId, user, 1);
			aWakes.countDown();
			aChecking.get();
		}

		assertThat(stock.available(eventId)).hasValue(7);
		assertStoresAgree(eventId);
		assertThat(a.verified()).isFalse();
	}

	@Test
	void onlyTheCurrentOwnerOfTheWipeLockCanDeleteRenewOrFinish() {
		String server = stock.serverRunId();
		String probe = "ticketrush:{event:-1}:stock";
		redis.opsForValue().set(RedisRestartGuard.SERVER_KEY, "the-server-before-the-restart");
		Duration lease = Duration.ofMinutes(1);

		assertThat(stock.checkServer(server, lease, "owner-1")).isEqualTo(RestartCheck.WIPE);
		assertThat(stock.checkServer(server, lease, "owner-2")).isEqualTo(RestartCheck.BUSY);
		redis.delete(RedisRestartGuard.WIPE_LOCK); // owner-1's lease runs out
		assertThat(stock.checkServer(server, lease, "owner-2")).isEqualTo(RestartCheck.WIPE);
		redis.opsForValue().set(probe, "5"); // owner-2 has rebuilt something

		// owner-1 wakes up: every step is refused and leaves owner-2's lock and data alone.
		assertThat(stock.unlinkOwned(List.of(probe), "owner-1", lease)).isFalse();
		assertThat(stock.trustServer(server, "owner-1")).isFalse();
		assertThat(redis.opsForValue().get(probe)).isEqualTo("5");
		assertThat(redis.opsForValue().get(RedisRestartGuard.WIPE_LOCK)).isEqualTo("owner-2");
		assertThat(redis.opsForValue().get(RedisRestartGuard.SERVER_KEY)).isEqualTo("the-server-before-the-restart");

		// owner-2's batches renew its lease; its trust frees the lock.
		redis.expire(RedisRestartGuard.WIPE_LOCK, Duration.ofMillis(500));
		assertThat(stock.unlinkOwned(List.of(probe), "owner-2", lease)).isTrue();
		assertThat(redis.getExpire(RedisRestartGuard.WIPE_LOCK)).isGreaterThan(30);
		assertThat(redis.hasKey(probe)).isFalse();
		assertThat(stock.trustServer(server, "owner-2")).isTrue();
		assertThat(redis.hasKey(RedisRestartGuard.WIPE_LOCK)).isFalse();
		assertThat(stock.checkServer(server, lease, "owner-3")).isEqualTo(RestartCheck.TRUSTED);
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
