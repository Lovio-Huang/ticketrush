package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.OptionalInt;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.config.TicketrushProperties.Reconciliation;
import com.yitaihuang.ticketrush.event.Event;
import com.yitaihuang.ticketrush.event.EventRepository;
import com.yitaihuang.ticketrush.order.Order;
import com.yitaihuang.ticketrush.order.OrderRepository;
import com.yitaihuang.ticketrush.order.OrderStatus;
import com.yitaihuang.ticketrush.reservation.StockGateway.Recount;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/** What the reconciler may touch, and what it must leave alone. */
class StockReconcilerTest {

	EventRepository events = mock();

	OrderRepository orders = mock();

	StockGateway stock = mock();

	StockRecovery recovery = mock();

	static final Instant NOW = Instant.parse("2026-10-28T12:00:00Z");

	SimpleMeterRegistry meters = new SimpleMeterRegistry();

	Event event = mock();

	StockReconciler reconciler;

	@BeforeEach
	void setUp() {
		TicketrushProperties properties = mock();
		when(properties.reconciliation()).thenReturn(new Reconciliation(Duration.ofSeconds(30), Duration.ofHours(1)));
		reconciler = new StockReconciler(events, orders, stock, recovery, properties, meters,
				Clock.fixed(NOW, ZoneOffset.UTC));
		when(event.getId()).thenReturn(1L);
		when(event.getTotalTickets()).thenReturn(10);
		when(stock.available(1L)).thenReturn(OptionalInt.of(7));
		when(stock.recount(1L, 10)).thenReturn(new Recount(7, 7, false));
	}

	/** An event that started {@code ago}, read at upkeep version 7, with nothing to fix and nothing unsent. */
	private void givenQuietEventStarted(Duration ago) {
		when(events.findNeedingUpkeep(NOW)).thenReturn(List.of(event));
		when(event.getStartsAt()).thenReturn(NOW.minus(ago));
		when(event.getUpkeepVersion()).thenReturn(7L);
	}

	@Test
	void anEventWithNothingLeftToDoAnHourAfterItStartedIsSettledAtTheVersionItWasReadAt() {
		givenQuietEventStarted(Duration.ofHours(1));

		reconciler.run();

		verify(events).markSettled(1L, 7L, NOW);
	}

	@Test
	void anEventIsNotSettledWhileTheJobsStillHaveSomethingToDo() {
		givenQuietEventStarted(Duration.ofMinutes(59)); // too soon after the start
		reconciler.run();

		givenQuietEventStarted(Duration.ofHours(2));
		when(stock.outboxSize(1L)).thenReturn(1L); // an accepted order Kafka hasn't confirmed
		reconciler.run();

		when(stock.outboxSize(1L)).thenReturn(0L);
		Order cancelled = order("cancelled", OrderStatus.CANCELLED);
		when(stock.heldOrderIds(1L)).thenReturn(List.of("cancelled"));
		when(orders.findAllById(List.of("cancelled"))).thenReturn(List.of(cancelled));
		when(stock.release(1L, "cancelled")).thenReturn(true); // this run still had to release one
		reconciler.run();

		when(stock.release(1L, "cancelled")).thenReturn(false);
		when(stock.available(1L)).thenReturn(OptionalInt.empty()); // Redis lost it: rebuilt first
		reconciler.run();

		verify(events, never()).markSettled(anyLong(), anyLong(), any());
	}

	private static Order order(String id, OrderStatus status) {
		Order order = mock();
		when(order.getId()).thenReturn(id);
		when(order.getStatus()).thenReturn(status);
		return order;
	}

	@Test
	void ordersMysqlHasFinishedAreReleased() {
		List<Order> finished = List.of(order("cancelled", OrderStatus.CANCELLED), order("expired", OrderStatus.EXPIRED),
				order("rejected", OrderStatus.REJECTED));
		when(stock.heldOrderIds(1L)).thenReturn(List.of("cancelled", "expired", "rejected"));
		when(orders.findAllById(List.of("cancelled", "expired", "rejected"))).thenReturn(finished);
		when(stock.release(anyLong(), anyString())).thenReturn(true);

		reconciler.reconcile(event);

		verify(stock).release(1L, "cancelled");
		verify(stock).release(1L, "expired");
		verify(stock).release(1L, "rejected");
		assertThat(meters.counter("ticketrush.stock.late_releases").count()).isEqualTo(3);
	}

	@Test
	void activeOrdersAndOrdersStillInFlightAreLeftAlone() {
		// "in-flight" is held in Redis but not in MySQL yet: the consumer hasn't written it.
		List<Order> active = List.of(order("pending", OrderStatus.PENDING_PAYMENT), order("paid", OrderStatus.PAID));
		when(stock.heldOrderIds(1L)).thenReturn(List.of("pending", "paid", "in-flight"));
		when(orders.findAllById(List.of("pending", "paid", "in-flight"))).thenReturn(active);

		reconciler.reconcile(event);

		verify(stock, never()).release(anyLong(), anyString());
	}

	@Test
	void withoutDriftTheExpensiveRecountIsSkipped() {
		when(stock.drifted(1L, 10)).thenReturn(false);

		reconciler.reconcile(event);

		verify(stock, never()).recount(anyLong(), org.mockito.ArgumentMatchers.anyInt());
	}

	@Test
	void stockIsRecountedFromHeldOrdersNotCopiedFromMysql() {
		when(stock.drifted(1L, 10)).thenReturn(true);
		when(stock.recount(1L, 10)).thenReturn(new Recount(5, 7, false));

		reconciler.reconcile(event);

		verify(stock).recount(1L, 10);
		verify(event, never()).getAvailableTickets();
		assertThat(meters.counter("ticketrush.stock.corrections").count()).isEqualTo(1);
	}

	@Test
	void aRecountThatChangesNothingIsNotCounted() {
		when(stock.drifted(1L, 10)).thenReturn(true);
		reconciler.reconcile(event);

		assertThat(meters.counter("ticketrush.stock.corrections").count()).isZero();
	}

	@Test
	void aMissingStockKeyTriggersARebuildInsteadOfARecount() {
		when(stock.available(1L)).thenReturn(OptionalInt.empty());

		reconciler.reconcile(event);

		verify(recovery).rebuild(1L);
		verify(stock, never()).recount(anyLong(), org.mockito.ArgumentMatchers.anyInt());
	}

}
