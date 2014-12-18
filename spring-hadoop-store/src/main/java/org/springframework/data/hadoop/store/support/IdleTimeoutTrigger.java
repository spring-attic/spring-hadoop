/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.store.support;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;
import org.springframework.util.Assert;

/**
 * A {@link Trigger} for idle timeout task execution.
 *
 * @author Janne Valkealahti
 *
 */
public class IdleTimeoutTrigger implements Trigger {

	private volatile long timeout;

	private final TimeUnit timeUnit;

	private volatile long initialDelay = 0;

	private volatile long lastCompletion = Long.MIN_VALUE;

	private volatile long lastReset = Long.MIN_VALUE;

	/**
	 * Instantiates a new idle timeout trigger.
	 *
	 * @param timeout the timeout
	 */
	public IdleTimeoutTrigger(long timeout) {
		this(timeout, null);
	}

	/**
	 * Instantiates a new idle timeout trigger.
	 *
	 * @param timeout the timeout
	 * @param timeUnit the time unit
	 */
	public IdleTimeoutTrigger(long timeout, TimeUnit timeUnit) {
		Assert.isTrue(timeout >= 0, "timeout must not be negative");
		this.timeUnit = (timeUnit != null ? timeUnit : TimeUnit.MILLISECONDS);
		this.timeout = this.timeUnit.toMillis(timeout);
	}

	@Override
	public Date nextExecutionTime(TriggerContext triggerContext) {
		if (triggerContext.lastScheduledExecutionTime() == null) {
			return new Date(System.currentTimeMillis() + initialDelay);
		}
		if (lastReset > lastCompletion) {
			lastCompletion = triggerContext.lastCompletionTime().getTime();
			return new Date(lastReset + timeout);
		}
		else {
			lastCompletion = triggerContext.lastCompletionTime().getTime();
			return new Date(lastCompletion + timeout);
		}
	}

	/**
	 * Specify the delay for the initial execution. It will be evaluated in terms of this trigger's {@link TimeUnit}. If
	 * no time unit was explicitly provided upon instantiation, the default is milliseconds.
	 *
	 * @param initialDelay initial delay
	 */
	public void setInitialDelay(long initialDelay) {
		this.initialDelay = timeUnit.toMillis(initialDelay);
	}

	/**
	 * Notify trigger that we should get into new timeout period. This setups a new trigger time to happen after next
	 * idle period.
	 */
	public void reset() {
		lastReset = System.currentTimeMillis();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (initialDelay ^ (initialDelay >>> 32));
		result = prime * result + (int) (timeout ^ (timeout >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IdleTimeoutTrigger other = (IdleTimeoutTrigger) obj;
		if (initialDelay != other.initialDelay)
			return false;
		if (timeout != other.timeout)
			return false;
		return true;
	}

}
