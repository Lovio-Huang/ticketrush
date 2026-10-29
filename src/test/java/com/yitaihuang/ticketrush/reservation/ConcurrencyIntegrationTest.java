package com.yitaihuang.ticketrush.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.yitaihuang.ticketrush.AbstractIntegrationTest;
import com.yitaihuang.ticketrush.common.ApiException;
import com.yitaihuang.ticketrush.event.EventRepository;
import com.yitaihuang.ticketrush.order.Order;
import com.yitaihuang.ticketrush.order.OrderRepository;
import com.yitaihuang.ticketrush.order.OrderService;
import com.yitaihuang.ticketrush.order.OrderStatus;

class ConcurrencyIntegrationTest extends AbstractIntegrationTest {

	@Autowired
	ReservationService reservations;

	@Autowired
	StockGateway stock;

	@Autowired
	EventRepository events;

	@Autowired
	OrderRepository orders;

	@Autowired
	StockReconciler reconciler;

	@Autowired
	OrderService orderService;

	@Test
	void sellsExactlyTheAvailableTicketsWhenThousandsRaceForThem() throws Exception {
		int tickets = 100;
		int buyers = 1_000;
		long eventId = createEvent(tickets, 1);

		Map<String, Long> outcomes = raceToReserve(eventId, buyers, i -> "buyer-" + i);

		assertThat(outcomes).containsOnly(Map.entry("ACCEPTED", (long) tickets),
				Map.entry("SOLD_OUT", (long) (buyers - tickets)));
		await().atMost(Duration.ofSeconds(30))
			.untilAsserted(() -> assertThat(orders.countByEventIdAndStatus(eventId, OrderStatus.PENDING_PAYMENT))
				.isEqualTo(tickets));
		assertThat(events.findById(eventId).orElseThrow().getAvailableTickets()).isZero();
		assertThat(stock.available(eventId)).hasValue(0);
		awaitStoresAgree(eventId);
	}

	@Test
	void enforcesThePerUserLimitWhenOneUserFiresManyParallelRequests() throws Exception {
		long eventId = createEvent(100, 2);

		// Same user, distinct idempotency keys: a script trying to grab more tickets.
		Map<String, Long> outcomes = raceToReserve(eventId, 8, i -> "scalper");

		assertThat(outcomes.get("ACCEPTED")).isEqualTo(2L);
		assertThat(outcomes.get("PURCHASE_LIMIT_EXCEEDED")).isEqualTo(6L);
		assertThat(stock.available(eventId)).hasValue(98);
		awaitStoresAgree(eventId);
	}

	@Test
	void reconcilingNonStopDuringASaleWithCancellationsNeverSellsATicketTwice() throws Exception {
		long eventId = createEvent(50, 1);
		AtomicBoolean selling = new AtomicBoolean(true);
		try (ExecutorService background = Executors.newVirtualThreadPerTaskExecutor()) {
			Future<?> reconciling = background.submit(() -> {
				while (selling.get()) {
					reconciler.run();
				}
			});

			assertThat(raceToReserve(eventId, 300, i -> "wave1-" + i).get("ACCEPTED")).isEqualTo(50L);
			await().atMost(Duration.ofSeconds(30)).until(() -> orders.findByEventId(eventId).size() == 50);

			// Second wave: 10 buyers cancel while 100 newcomers race for whatever comes back.
			List<Order> cancelled = orders.findByEventId(eventId).subList(0, 10);
			Future<Map<String, Long>> secondWave = background.submit(() -> raceToReserve(eventId, 100, i -> "wave2-" + i));
			for (Order order : cancelled) {
				orderService.cancel(order.getId(), order.getUserId());
			}
			long resold = secondWave.get().getOrDefault("ACCEPTED", 0L);

			selling.set(false);
			reconciling.get();
			assertThat(resold).isLessThanOrEqualTo(10);
			await().atMost(Duration.ofSeconds(30))
				.until(() -> orders.findByEventId(eventId).size() == 50 + resold);
		}
		awaitStoresAgree(eventId);
		long active = orders.findByEventId(eventId).stream().filter(o -> o.getStatus().holdsTickets()).count();
		assertThat(active).isLessThanOrEqualTo(50);
	}

	private Map<String, Long> raceToReserve(long eventId, int requests, Function<Integer, String> userForRequest)
			throws Exception {
		CountDownLatch startingGun = new CountDownLatch(1);
		List<Future<String>> results = new ArrayList<>();
		try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
			for (int i = 0; i < requests; i++) {
				String userId = userForRequest.apply(i);
				String idempotencyKey = "key-" + i;
				results.add(pool.submit(() -> {
					startingGun.await();
					try {
						reservations.reserve(eventId, userId, 1, idempotencyKey);
						return "ACCEPTED";
					}
					catch (ApiException ex) {
						return ex.getCode();
					}
				}));
			}
			startingGun.countDown();
		}
		List<String> outcomes = new ArrayList<>();
		for (Future<String> result : results) {
			outcomes.add(result.get());
		}
		return outcomes.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
	}

}
