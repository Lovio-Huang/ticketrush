package com.yitaihuang.ticketrush.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.yitaihuang.ticketrush.common.ApiException;

class OrderTest {

	private static final Instant NOW = Instant.parse("2027-03-01T12:00:00Z");

	private static final Duration WINDOW = Duration.ofMinutes(10);

	private final Order order = Order.pendingPayment(
			new OrderRequested("order-1", 7L, "alice", 2, 5000, "key-1", NOW), NOW, WINDOW);

	@Test
	void canBePaidInsideThePaymentWindow() {
		order.pay(NOW.plus(WINDOW).minusSeconds(1));

		assertThat(order.getStatus()).isEqualTo(OrderStatus.PAID);
	}

	@Test
	void cannotBePaidOnceTheWindowHasClosed() {
		assertThatThrownBy(() -> order.pay(NOW.plus(WINDOW))).isInstanceOf(ApiException.class)
			.hasFieldOrPropertyWithValue("code", "ORDER_EXPIRED");
		assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING_PAYMENT);
	}

	@Test
	void aPaidOrderCannotBeCancelledOrExpired() {
		order.pay(NOW);

		assertThatThrownBy(() -> order.cancel(NOW)).hasFieldOrPropertyWithValue("code", "INVALID_ORDER_STATE");
		assertThatThrownBy(() -> order.expire(NOW)).hasFieldOrPropertyWithValue("code", "INVALID_ORDER_STATE");
	}

	@Test
	void onlyTheBuyerOwnsTheOrder() {
		assertThat(order.isOwnedBy("alice")).isTrue();
		assertThat(order.isOwnedBy("mallory")).isFalse();
	}

}
