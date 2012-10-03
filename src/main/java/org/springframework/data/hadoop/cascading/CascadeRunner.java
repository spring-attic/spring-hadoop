/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.hadoop.cascading;

import java.util.concurrent.Callable;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import cascading.cascade.Cascade;
import cascading.flow.Flow;
import cascading.management.UnitOfWork;
import cascading.stats.CascadingStats;

/**
 * Simple runner for executing {@link Cascade}s or {@link Flow}s. By default, the runner waits for the jobs to finish and returns its status.
 * 
 * @author Costin Leau
 */
public class CascadeRunner implements InitializingBean, DisposableBean, Callable<CascadingStats> {

	private boolean waitToComplete = true;
	private boolean runAtStartup = true;
	private UnitOfWork<CascadingStats> uow;

	private Iterable<Callable<?>> preActions;
	private Iterable<Callable<?>> postActions;

	@Override
	public void afterPropertiesSet() throws Exception {
		if (runAtStartup) {
			call();
		}
	}

	@Override
	public void destroy() {
		if (uow != null) {
			uow.stop();
		}
	}

	@Override
	public CascadingStats call() throws Exception {
		invoke(preActions);
		CascadingStats stats = Runner.run(uow, waitToComplete);
		invoke(postActions);

		return stats;
	}

	/**
	 * Sets the unit of work. Can be of type {@link Flow} or {@link Cascade}.
	 *
	 * @param uow the new unit of work.
	 */
	public void setUnitOfWork(UnitOfWork<CascadingStats> uow) {
		this.uow = uow;
	}

	/**
	 * Sets the run at startup.
	 *
	 * @param runAtStartup The runAtStartup to set.
	 */
	public void setRunAtStartup(boolean runAtStartup) {
		this.runAtStartup = runAtStartup;
	}

	/**
	 * Actions to be invoked before running the action.
	 * 
	 * @param beans
	 */
	public void setPreAction(Iterable<Callable<?>> actions) {
		this.preActions = actions;
	}

	/**
	 * Actions to be invoked after running the action.
	 * 
	 * @param beans
	 */
	public void setPostAction(Iterable<Callable<?>> actions) {
		this.postActions = actions;
	}

	private void invoke(Iterable<Callable<?>> actions) throws Exception {
		if (actions != null) {
			for (Callable<?> action : actions) {
				action.call();
			}
		}
	}
}