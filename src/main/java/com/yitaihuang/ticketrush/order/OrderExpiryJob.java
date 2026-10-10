package com.yitaihuang.ticketrush.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.yitaihuang.ticketrush.config.TicketrushProperties;

/** Puts tickets from unpaid orders back on sale once their payment window closes. */
@Component
public class OrderExpiryJob {

	private static final Logger log = LoggerFactory.getLogger(OrderExpiryJob.class);

	private final OrderService orders;

	private final int batchSize;

	public OrderExpiryJob(OrderService orders, TicketrushProperties properties) {
		this.orders = orders;
		this.batchSize = properties.orders().expiryBatchSize();
	}

	@Scheduled(fixedDelayString = "${ticketrush.orders.expiry-scan-interval}")
	public void run() {
		// Small batches keep each transaction (and its row locks) short.
		int total = 0;
		int expired;
		do {
			expired = orders.expireOverdue(batchSize);
			total += expired;
		}
		while (expired == batchSize);
		if (total > 0) {
			log.info("Expired {} unpaid orders", total);
		}
	}

}
