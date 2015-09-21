/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.store.dataset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.hadoop.store.support.IdleTimeoutTrigger;
import org.springframework.data.hadoop.store.support.LifecycleObjectSupport;
import org.springframework.data.hadoop.store.support.PollingTaskSupport;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;

/**
 * Support class adding timeout poller functionality.
 *
 * @author Janne Valkealahti
 *
 */
public class DatasetStoreObjectSupport extends LifecycleObjectSupport {

	private static final Log log = LogFactory.getLog(DatasetStoreObjectSupport.class);

	/** Poller checking idle timeouts */
	private IdleTimeoutPoller idlePoller;

	/** Trigger in poller */
	private volatile IdleTimeoutTrigger idleTrigger;

	/** Poller checking idle timeouts */
	private CloseTimeoutPoller closePoller;

	/** Trigger in poller */
	private volatile IdleTimeoutTrigger closeTrigger;

	/**
	 * In millis last idle time reset. We explicitly use negative value to indicate reset state
	 * because we can't use long max value which would flip if adding something. We reset this
	 * state when idle timeout happens so that we can wait next idle time.
	 */
	private volatile long lastIdle = Long.MIN_VALUE;

	/** In millis an idle timeout for writer/reader. */
	private volatile long idleTimeout;

	/** In millis a close timeout for writer/reader. */
	private volatile long closeTimeout;

	@Override
	protected void onInit() throws Exception {
		// if we have timeout, enable polling by creating it
		if (idleTimeout > 0) {
			idleTrigger = new IdleTimeoutTrigger(idleTimeout);
			idlePoller = new IdleTimeoutPoller(getTaskScheduler(), getTaskExecutor(), idleTrigger);
			idlePoller.init();
		}
		// if we have close timeout, setup trigger with delay
		if (closeTimeout > 0) {
			closeTrigger = new IdleTimeoutTrigger(closeTimeout);
			closeTrigger.setInitialDelay(closeTimeout);
			closePoller = new CloseTimeoutPoller(getTaskScheduler(), getTaskExecutor(), closeTrigger);
			closePoller.init();
		}
	}

	@Override
	protected void doStart() {
		if (idlePoller != null) {
			idlePoller.start();
		}
		if (closePoller != null) {
			closePoller.start();
		}
	}

	@Override
	protected void doStop() {
		if (idlePoller != null) {
			idlePoller.stop();
		}
		idlePoller = null;
		if (closePoller != null) {
			closePoller.stop();
		}
		closePoller = null;
	}

	/**
	 * Sets the idle timeout.
	 *
	 * @param idleTimeout the new idle timeout
	 */
	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	/**
	 * Sets the close timeout.
	 *
	 * @param closeTimeout the new close timeout
	 */
	public void setCloseTimeout(long closeTimeout) {
		this.closeTimeout = closeTimeout;
	}

	/**
	 * Reset idle timeout.
	 */
	public void resetIdleTimeout() {
		lastIdle = System.currentTimeMillis();
	}

	/**
	 * Handle idle timeout. This method should be overriden
	 * to be notified of idle timeouts. Default implementation
	 * doesn't do anything.
	 */
	protected void handleTimeout() {
	}

	/**
	 * Poller which checks idle timeout by last write and closes a writer if timeout has occurred.
	 */
	private class IdleTimeoutPoller extends PollingTaskSupport<Boolean> {

		public IdleTimeoutPoller(TaskScheduler taskScheduler, TaskExecutor taskExecutor, Trigger trigger) {
			super(taskScheduler, taskExecutor, trigger);
		}

		@Override
		protected Boolean doPoll() {
			if (log.isDebugEnabled()) {
				log.debug("Polling idle timeout with idleTimeout=" + idleTimeout + " lastIdle=" + lastIdle);
			}
			// return true if we've been idle too long
			return idleTimeout > 0 && lastIdle > 0 && lastIdle + idleTimeout < System.currentTimeMillis();
		}

		@Override
		protected void onPollResult(Boolean result) {
			if (result) {
				try {
					if (log.isDebugEnabled()) {
						log.debug("Idle timeout detected, calling handleTimeout()");
					}
					handleTimeout();
				} catch (Exception e) {
					// TODO: handle error
					log.error("error closing", e);
				} finally {
					// reset lastIdle so we can wait new timeout
					lastIdle = Long.MIN_VALUE;
				}
			}
		}

	}

	/**
	 * Poller which gets called by a close timeout and closes a writer.
	 */
	private class CloseTimeoutPoller extends PollingTaskSupport<Void> {

		public CloseTimeoutPoller(TaskScheduler taskScheduler, TaskExecutor taskExecutor, Trigger trigger) {
			super(taskScheduler, taskExecutor, trigger);
		}

		@Override
		protected Void doPoll() {
			try {
				if (log.isDebugEnabled()) {
					log.debug("Close timeout detected, calling handleTimeout()");
				}
				handleTimeout();
			} catch (Exception e) {
				// TODO: handle error
				log.error("error closing", e);
			}
			return null;
		}
	}

}
