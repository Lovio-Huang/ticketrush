package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;

import com.yitaihuang.ticketrush.reservation.StockGateway.RestartCheck;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/** When the guard's verdict counts, on a clock the test moves by hand. */
class RedisRestartGuardTest {

	private static final Duration MAX_AGE = Duration.ofSeconds(2);

	private final StockGateway stock = mock();

	private final AtomicLong nanos = new AtomicLong(1_000_000_000L);

	private final RedisRestartGuard guard = new RedisRestartGuard(stock, new SimpleMeterRegistry(),
			Duration.ofSeconds(1), MAX_AGE, nanos::get);

	@Test
	void nothingIsTrustedBeforeTheFirstCheck() {
		assertThat(guard.verified()).isFalse();
	}

	@Test
	void aPassingCheckCountsForTheMaxAgeAndNoLonger() {
		givenServer(RestartCheck.TRUSTED);
		guard.run();
		assertThat(guard.verified()).isTrue();

		advance(MAX_AGE);
		assertThat(guard.verified()).isTrue();
		advance(Duration.ofNanos(1));
		assertThat(guard.verified()).as("no check since").isFalse();

		guard.run();
		assertThat(guard.verified()).isTrue();
	}

	@Test
	void theVerdictAgesFromWhenItsCheckStarted() {
		when(stock.serverRunId()).thenAnswer(call -> {
			advance(Duration.ofMillis(1_500)); // a slow answer
			return "run-1";
		});
		when(stock.checkServer(anyString(), any(), anyString())).thenReturn(RestartCheck.TRUSTED);

		guard.run();
		advance(Duration.ofMillis(600));

		assertThat(guard.verified()).isFalse();
	}

	@Test
	void aFailedCheckWithdrawsTrustAtOnce() {
		givenServer(RestartCheck.TRUSTED);
		guard.run();
		when(stock.serverRunId()).thenThrow(new QueryTimeoutException("Redis command timed out"));

		guard.run();

		assertThat(guard.verified()).isFalse();
	}

	@Test
	void whileAnotherInstanceWipesNothingIsTrusted() {
		givenServer(RestartCheck.TRUSTED);
		guard.run();
		givenServer(RestartCheck.BUSY);

		guard.run();

		assertThat(guard.verified()).isFalse();
	}

	@Test
	void aWipeThatLostItsLeaseRefreshesNothing() {
		givenServer(RestartCheck.WIPE);
		when(stock.wipeAllEvents(anyString(), any())).thenReturn(OptionalInt.empty());

		guard.run();

		assertThat(guard.verified()).isFalse();
	}

	@Test
	void aFinishedWipeIsTrustedFromWhenItsCheckStarted() {
		givenServer(RestartCheck.WIPE);
		when(stock.wipeAllEvents(anyString(), any())).thenAnswer(call -> {
			advance(Duration.ofSeconds(3)); // a long wipe
			return OptionalInt.of(42);
		});
		when(stock.trustServer(anyString(), anyString())).thenReturn(true);

		guard.run();

		assertThat(guard.verified()).as("trusted, but as of the check's start: already too old").isFalse();
	}

	private void givenServer(RestartCheck result) {
		when(stock.serverRunId()).thenReturn("run-1");
		when(stock.checkServer(anyString(), any(), anyString())).thenReturn(result);
	}

	private void advance(Duration by) {
		nanos.addAndGet(by.toNanos());
	}

}
