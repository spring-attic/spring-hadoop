/*
 * Copyright 2013-2015 the original author or authors.
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
package org.springframework.data.hadoop.store.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;

/**
 * Base implementation of a store objects sharing a common functionality among store formats.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class StoreObjectSupport extends LifecycleObjectSupport {

	private final static Log log = LogFactory.getLog(StoreObjectSupport.class);

	/** Hadoop configuration */
	private final Configuration configuration;

	/** Codec info for store */
	private final CodecInfo codecInfo;

	/** Hdfs path into a store */
	private final Path basePath;

	/** Poller checking idle timeouts */
	private IdleTimeoutPoller idlePoller;

	/** Trigger in poller */
	private volatile IdleTimeoutTrigger idleTrigger;

	/** Poller checking idle timeouts */
	private CloseTimeoutPoller closePoller;

	/** Trigger in poller */
	private volatile IdleTimeoutTrigger closeTrigger;

	/** Poller checking flush timeouts */
	private FlushTimeoutPoller flushPoller;

	/** Trigger in flush poller */
	private volatile IdleTimeoutTrigger flushTrigger;

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

	/** In millis a flush timeout for writer/reader. */
	private volatile long flushTimeout;

	/**
	 * Instantiates a new abstract store support.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 */
	public StoreObjectSupport(Configuration configuration, Path basePath, CodecInfo codec) {
		this.configuration = configuration;
		this.basePath = basePath;
		this.codecInfo = codec;
	}

	@Override
	protected void onInit() throws Exception {
		// if we have idle timeout, enable polling by creating it
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
		// if we have flush timeout, setup trigger with delay
		if (flushTimeout > 0) {
			flushTrigger = new IdleTimeoutTrigger(flushTimeout);
			flushTrigger.setInitialDelay(flushTimeout);
			flushPoller = new FlushTimeoutPoller(getTaskScheduler(), getTaskExecutor(), flushTrigger);
			flushPoller.init();
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
		if (flushPoller != null) {
			flushPoller.start();
		}
	}

	@Override
	protected void doStop() {
		// stop flush before others
		if (flushPoller != null) {
			flushPoller.stop();
		}
		flushPoller = null;
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
	 * Gets the configuration.
	 *
	 * @return the configuration
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Gets the path.
	 *
	 * @return the path
	 */
	public Path getPath() {
		return basePath;
	}

	/**
	 * Gets the codec.
	 *
	 * @return the codec
	 */
	public CodecInfo getCodec() {
		return codecInfo;
	}

	/**
	 * Checks if is compressed.
	 *
	 * @return true, if is compressed
	 */
	public boolean isCompressed() {
		return codecInfo != null;
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
	 * Sets the flush timeout.
	 *
	 * @param flushTimeout the new flush timeout
	 */
	public void setFlushTimeout(long flushTimeout) {
		this.flushTimeout = flushTimeout;
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
	 * Handle flush timeout. This method should be overriden
	 * to be notified of flush timeouts. Default implementation
	 * doesn't do anything.
	 */
	protected void flushTimeout() {
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

	/**
	 * Poller which gets called by a flush timeout and flushes a writer.
	 */
	private class FlushTimeoutPoller extends PollingTaskSupport<Void> {

		public FlushTimeoutPoller(TaskScheduler taskScheduler, TaskExecutor taskExecutor, Trigger trigger) {
			super(taskScheduler, taskExecutor, trigger);
		}

		@Override
		protected Void doPoll() {
			try {
				if (log.isDebugEnabled()) {
					log.debug("Flush timeout detected, calling flushTimeout()");
				}
				flushTimeout();
			} catch (Exception e) {
				// TODO: handle error
				log.error("error flushing", e);
			}
			return null;
		}
	}

}
