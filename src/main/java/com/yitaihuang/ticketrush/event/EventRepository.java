package com.yitaihuang.ticketrush.event;

import java.time.Instant;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

public interface EventRepository extends JpaRepository<Event, Long> {

	/**
	 * Conditional decrement: the WHERE clause makes check-and-update a single
	 * atomic statement, so MySQL can never go below zero even if Redis and MySQL
	 * disagree. Returns the number of rows updated (0 means not enough tickets).
	 */
	@Modifying
	@Query("update Event e set e.availableTickets = e.availableTickets - :quantity, " + REOPEN
			+ " where e.id = :eventId and e.availableTickets >= :quantity")
	int decrementAvailable(long eventId, int quantity);

	@Modifying
	@Query("update Event e set e.availableTickets = e.availableTickets + :quantity, " + REOPEN
			+ " where e.id = :eventId")
	int incrementAvailable(long eventId, int quantity);

	/**
	 * Every write that may need the background jobs later (a stock change, a
	 * rejected order, a dead letter, a void) reopens the event in the same
	 * transaction: it is visited again, and a reconciler that checked it before
	 * this write can't settle it (see {@link #markSettled}).
	 */
	String REOPEN = "e.settledAt = null, e.upkeepVersion = e.upkeepVersion + 1";

	@Transactional
	@Modifying
	@Query("update Event e set " + REOPEN + " where e.id = :eventId")
	int touch(long eventId);

	/** Events that haven't happened yet, soonest sale first. */
	Page<Event> findByStartsAtAfterOrderBySaleStartsAtAsc(Instant now, Pageable pageable);

	/**
	 * Events the background jobs must visit: on sale now, or whose sale ended but
	 * which aren't settled yet (orders accepted before the end may still need
	 * sending, storing or releasing). Settled events drop out for good, so this
	 * stays small however many events have taken place.
	 */
	@Query("select e from Event e where e.saleStartsAt <= :now and e.settledAt is null")
	List<Event> findNeedingUpkeep(Instant now);

	/** Settles the event only if nothing has reopened it since its version was read. */
	@Transactional
	@Modifying
	@Query("update Event e set e.settledAt = :now "
			+ "where e.id = :eventId and e.settledAt is null and e.upkeepVersion = :seenVersion")
	int markSettled(long eventId, long seenVersion, Instant now);

}
