/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.hadoop.mapreduce;

import java.util.Collection;
import java.util.concurrent.Callable;

import org.apache.hadoop.mapreduce.Job;

/**
 * Simple runner for submitting Hadoop jobs sequentially. By default, the runner waits for the jobs to finish and returns a boolean indicating
 * whether all the jobs succeeded or not (when there's no waiting, the status cannot be determined and null is returned).
 * <p>
 * For more control over the job execution and outcome consider querying the {@link Job}s or using Spring Batch (see the reference documentation for more info).
 * <p>
 * To make the runner execute at startup, use {@link #setRunAtStartup(boolean)}.
 * 
 * @author Costin Leau
 */
public class JobRunner extends JobExecutor implements Callable<Void> {

	private boolean runAtStartup = false;
	private Iterable<Callable<?>> preActions;
	private Iterable<Callable<?>> postActions;

	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();

		if (runAtStartup) {
			call();
		}
	}

	@Override
	public Void call() throws Exception {
		// pre action
		invoke(preActions);
		startJobs();
		// post action
		invoke(postActions);
		return null;
	}

	/**
	 * Indicates whether the jobs should be submitted at startup (default) or not.
	 * 
	 * @param runAtStartup The runAtStartup to set.
	 */
	public void setRunAtStartup(boolean runAtStartup) {
		this.runAtStartup = runAtStartup;
	}

	/**
	 * Actions to be invoked before running the action.
	 * 
	 * @param actions actions
	 */
	public void setPreAction(Collection<Callable<?>> actions) {
		this.preActions = actions;
	}

	/**
	 * Actions to be invoked after running the action.
	 * 
	 * @param actions actions
	 */
	public void setPostAction(Collection<Callable<?>> actions) {
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