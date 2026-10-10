package com.yitaihuang.ticketrush.common;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Defers a side effect (usually a Redis write) until the surrounding database
 * transaction has committed, so a rollback never leaves Redis ahead of MySQL.
 */
public final class AfterCommit {

	private AfterCommit() {
	}

	public static void run(Runnable action) {
		if (!TransactionSynchronizationManager.isSynchronizationActive()) {
			action.run();
			return;
		}
		TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
			@Override
			public void afterCommit() {
				action.run();
			}
		});
	}

}
