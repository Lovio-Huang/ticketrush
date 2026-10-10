package com.yitaihuang.ticketrush.order;

import java.time.Instant;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface OrderRepository extends JpaRepository<Order, String> {

	/**
	 * Locks a batch of overdue orders. SKIP LOCKED lets several app instances run
	 * the expiry job at once: each one grabs a different batch instead of
	 * blocking on rows another instance is already expiring.
	 */
	@Query(value = """
			SELECT * FROM orders
			WHERE status = 'PENDING_PAYMENT' AND expires_at <= :now
			ORDER BY expires_at
			LIMIT :limit
			FOR UPDATE SKIP LOCKED
			""", nativeQuery = true)
	List<Order> lockOverdue(Instant now, int limit);

	long countByEventIdAndStatus(long eventId, OrderStatus status);

}
