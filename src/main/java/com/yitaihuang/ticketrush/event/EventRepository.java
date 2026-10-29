package com.yitaihuang.ticketrush.event;

import java.time.Instant;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

public interface EventRepository extends JpaRepository<Event, Long> {

	/**
	 * Conditional decrement: the WHERE clause makes check-and-update a single
	 * atomic statement, so MySQL can never go below zero even if Redis and MySQL
	 * disagree. Returns the number of rows updated (0 means not enough tickets).
	 */
	@Modifying
	@Query("update Event e set e.availableTickets = e.availableTickets - :quantity "
			+ "where e.id = :eventId and e.availableTickets >= :quantity")
	int decrementAvailable(long eventId, int quantity);

	@Modifying
	@Query("update Event e set e.availableTickets = e.availableTickets + :quantity where e.id = :eventId")
	int incrementAvailable(long eventId, int quantity);

	/** Events that haven't happened yet, soonest sale first. */
	Page<Event> findByStartsAtAfterOrderBySaleStartsAtAsc(Instant now, Pageable pageable);

	/** Events whose sale is open right now: the ones whose stock lives in Redis. */
	@Query("select e from Event e where e.saleStartsAt <= :now and e.startsAt > :now")
	List<Event> findOnSale(Instant now);

}
