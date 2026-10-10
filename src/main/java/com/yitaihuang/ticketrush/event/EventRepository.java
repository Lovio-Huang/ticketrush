package com.yitaihuang.ticketrush.event;

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

}
