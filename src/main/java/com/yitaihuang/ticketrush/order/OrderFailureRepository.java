package com.yitaihuang.ticketrush.order;

import java.util.List;
import java.util.Optional;

import jakarta.persistence.LockModeType;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

public interface OrderFailureRepository extends JpaRepository<OrderFailure, Long> {

	boolean existsByDltPositionOrLastDltPosition(String dltPosition, String lastDltPosition);

	@Lock(LockModeType.PESSIMISTIC_WRITE)
	@Query("select f from OrderFailure f where f.orderId = :orderId")
	Optional<OrderFailure> lockByOrderId(String orderId);

	@Lock(LockModeType.PESSIMISTIC_WRITE)
	@Query("select f from OrderFailure f where f.id = :id")
	Optional<OrderFailure> lockById(long id);

	List<OrderFailure> findTop100ByStatusOrderByIdDesc(OrderFailure.Status status);

	List<OrderFailure> findTop100ByOrderByIdDesc();

	long countByStatus(OrderFailure.Status status);

}
