/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.yarn.support;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.util.Assert;

/**
 * Helper class to ease working with polling tasks executed using Spring {@code TaskScheduler} and {@code TaskExecutor}.
 *
 * @author Janne Valkealahti
 * @param <T> the return type of poll task
 *
 */
public abstract class PollingTaskSupport<T> {

	private final static Log log = LogFactory.getLog(PollingTaskSupport.class);

	/** Trigger for polling task */
	private volatile Trigger trigger;

	/** Poller runnable */
	private volatile Runnable poller;

	/** Current running task if any */
	private volatile ScheduledFuture<?> runningTask;

	/** Spring task scheduler */
	private TaskScheduler taskScheduler;

	/** Spring task executor */
	private TaskExecutor taskExecutor;

	/**
	 * Instantiates a new polling task support. On default a simple {@code PeriodicTrigger} is used.
	 *
	 * @param taskScheduler the task scheduler
	 * @param taskExecutor the task executor
	 */
	public PollingTaskSupport(TaskScheduler taskScheduler, TaskExecutor taskExecutor) {
		this(taskScheduler, taskExecutor, TimeUnit.SECONDS, 5);
	}

	/**
	 * Instantiates a new polling task support. On default a simple {@code PeriodicTrigger} is used.
	 *
	 * @param taskScheduler the task scheduler
	 * @param taskExecutor the task executor
	 * @param unit the unit
	 * @param duration the duration
	 */
	public PollingTaskSupport(TaskScheduler taskScheduler, TaskExecutor taskExecutor, TimeUnit unit, long duration) {
		this.taskScheduler = taskScheduler;
		this.taskExecutor = taskExecutor;
		this.trigger = new PeriodicTrigger(unit.toMillis(duration));
	}

	/**
	 * Instantiates a new polling task support.
	 *
	 * @param taskScheduler the task scheduler
	 * @param taskExecutor the task executor
	 * @param trigger the trigger
	 */
	public PollingTaskSupport(TaskScheduler taskScheduler, TaskExecutor taskExecutor, Trigger trigger) {
		this.taskScheduler = taskScheduler;
		this.taskExecutor = taskExecutor;
		this.trigger = trigger;
	}


	/**
	 * Inits the poller.
	 */
	public void init() {
		Assert.notNull(taskScheduler, "Task scheduler must be set");
		Assert.notNull(taskExecutor, "Task executor must be set");
		poller = createPoller();
	}

	/**
	 * Starts the poller.
	 */
	public void start() {
		log.info("Scheduling poller with taskScheduler " + taskScheduler);
		runningTask = taskScheduler.schedule(poller, trigger);
	}

	/**
	 * Stops the poller.
	 */
	public void stop() {
		if (runningTask != null) {
			runningTask.cancel(true);
		}
		runningTask = null;
	}

	/**
	 * Sets the trigger.
	 *
	 * @param trigger the new trigger
	 */
	public void setTrigger(Trigger trigger) {
		this.trigger = trigger;
	}

	/**
	 * Do poll.
	 *
	 * @return the poll result
	 */
	protected abstract T doPoll();

	/**
	 * Callback on poll result. Default implementation doesn't do nothing.
	 *
	 * @param result the result
	 */
	protected void onPollResult(T result) {
	};

	/**
	 * Creates the poller.
	 *
	 * @return the runnable
	 */
	private Runnable createPoller() {
		Callable<T> pollingTask = new Callable<T>() {

			@Override
			public T call() throws Exception {
				return doPoll();
			}
		};
		return new Poller(pollingTask);
	}

	/**
	 * Internal helper class for poller.
	 */
	private class Poller implements Runnable {

		private final Callable<T> pollingTask;

		public Poller(Callable<T> pollingTask) {
			this.pollingTask = pollingTask;
		}

		@Override
		public void run() {
			taskExecutor.execute(new Runnable() {

				@Override
				public void run() {
					try {
						if (log.isDebugEnabled()) {
							log.debug("Running internal poller");
						}
						onPollResult(pollingTask.call());
					}
					catch (Exception e) {
						log.error("Error in polling task", e);
						throw new RuntimeException("Error executing polling task", e);
					}
				}
			});
		}
	}

}
