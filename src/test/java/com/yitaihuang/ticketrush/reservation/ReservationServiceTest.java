package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import com.yitaihuang.ticketrush.common.ApiException;
import com.yitaihuang.ticketrush.event.Event;
import com.yitaihuang.ticketrush.event.EventCatalog;
import com.yitaihuang.ticketrush.event.EventRepository;
import com.yitaihuang.ticketrush.event.EventSnapshot;
import com.yitaihuang.ticketrush.order.OrderRequested;
import com.yitaihuang.ticketrush.order.PendingOrderTracker;
import com.yitaihuang.ticketrush.reservation.StockGateway.Outcome;
import com.yitaihuang.ticketrush.reservation.StockGateway.Reservation;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class ReservationServiceTest {

	private static final Instant NOW = Instant.parse("2027-03-01T12:00:00Z");

	private static final EventSnapshot EVENT = new EventSnapshot(1L, "Concert", "Arena", NOW.plusSeconds(86_400),
			NOW.minusSeconds(60), 100, 2_500, 4);

	private final EventCatalog catalog = mock();

	private final EventRepository events = mock();

	private final StockGateway stock = mock();

	private final RateLimiter rateLimiter = mock();

	private final OrderRequestPublisher publisher = mock();

	private final PendingOrderTracker pendingOrders = mock();

	private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

	private ReservationService service;

	@BeforeEach
	void setUp() {
		service = new ReservationService(catalog, events, stock, rateLimiter, publisher, pendingOrders, meters,
				Clock.fixed(NOW, ZoneOffset.UTC));
		when(rateLimiter.tryAcquire(anyString())).thenReturn(true);
		when(catalog.get(1L)).thenReturn(EVENT);
	}

	@Test
	void acceptedReservationIsPublishedWithTheTotalPrice() {
		givenRedisAnswers(Outcome.RESERVED);

		ReservationService.Result result = service.reserve(1L, "alice", 2, "key-1");

		assertThat(result.replayed()).isFalse();
		verify(publisher).publish(new OrderRequested(result.orderId(), 1L, "alice", 2, 5_000, "key-1", NOW));
		verify(pendingOrders).markProcessing(result.orderId(), "alice");
		assertThat(meters.counter("ticketrush.reservations", "outcome", "accepted").count()).isEqualTo(1);
	}

	@Test
	void duplicateRequestReturnsTheOriginalOrderWithoutPublishingAgain() {
		when(stock.reserve(eq(1L), eq("alice"), eq(1), eq(4), eq("key-1"), anyString()))
			.thenReturn(new Reservation(Outcome.DUPLICATE, "original-order"));

		ReservationService.Result result = service.reserve(1L, "alice", 1, "key-1");

		assertThat(result).isEqualTo(new ReservationService.Result("original-order", true));
		verifyNoInteractions(publisher);
	}

	@Test
	void ticketsAreReturnedWhenKafkaIsUnavailable() {
		givenRedisAnswers(Outcome.RESERVED);
		doThrow(new RuntimeException("broker down")).when(publisher).publish(any());

		assertThatThrownBy(() -> service.reserve(1L, "alice", 2, "key-1")).isInstanceOf(ApiException.class)
			.hasFieldOrPropertyWithValue("status", HttpStatus.SERVICE_UNAVAILABLE);
		verify(stock).releaseAndForget(1L, "alice", 2, "key-1");
		verify(pendingOrders, never()).markProcessing(anyString(), anyString());
	}

	@Test
	void soldOutIsReportedAsConflict() {
		givenRedisAnswers(Outcome.SOLD_OUT);

		assertThatThrownBy(() -> service.reserve(1L, "alice", 1, "key-1"))
			.hasFieldOrPropertyWithValue("code", "SOLD_OUT");
		verifyNoInteractions(publisher);
	}

	@Test
	void rateLimitedUsersNeverReachRedisStock() {
		when(rateLimiter.tryAcquire("bot")).thenReturn(false);

		assertThatThrownBy(() -> service.reserve(1L, "bot", 1, "key-1"))
			.hasFieldOrPropertyWithValue("status", HttpStatus.TOO_MANY_REQUESTS);
		verifyNoInteractions(stock, publisher);
	}

	@Test
	void requestsBeforeTheSaleOpensAreRejected() {
		when(catalog.get(2L)).thenReturn(new EventSnapshot(2L, "Later", "Arena", NOW.plusSeconds(86_400),
				NOW.plusSeconds(3_600), 100, 2_500, 4));

		assertThatThrownBy(() -> service.reserve(2L, "alice", 1, "key-1"))
			.hasFieldOrPropertyWithValue("code", "SALE_NOT_STARTED");
		verifyNoInteractions(stock);
	}

	@Test
	void missingRedisStockIsReseededFromMysqlAndRetried() {
		Event persisted = mock();
		when(persisted.getAvailableTickets()).thenReturn(42);
		when(events.findById(1L)).thenReturn(Optional.of(persisted));
		when(stock.reserve(anyLong(), anyString(), anyInt(), anyInt(), anyString(), anyString()))
			.thenReturn(new Reservation(Outcome.NOT_INITIALIZED, ""))
			.thenAnswer(inv -> new Reservation(Outcome.RESERVED, inv.getArgument(5)));

		service.reserve(1L, "alice", 1, "key-1");

		verify(stock).initializeIfAbsent(1L, 42);
		verify(publisher).publish(any());
	}

	private void givenRedisAnswers(Outcome outcome) {
		when(stock.reserve(anyLong(), anyString(), anyInt(), anyInt(), anyString(), anyString()))
			.thenAnswer(inv -> new Reservation(outcome, outcome == Outcome.RESERVED ? inv.getArgument(5) : ""));
	}

}
