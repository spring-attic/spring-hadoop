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
package org.springframework.data.hadoop.mapreduce;

import java.util.Collection;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * Simple runner for submitting Hadoop jobs sequentially. By default, the runner waits for the jobs to finish and returns a boolean indicating
 * whether all the jobs succeeded or not (when there's no waiting, the status cannot be determined and null is returned).
 * 
 * <p/>
 * For more control over the job execution and outcome consider querying the {@link Job}s or using Spring Batch (see the reference documentation for more info).
 * <p/>Note by default, the runner is configured to execute at startup. One can customize this behaviour through {@link #setRunAtStartup(boolean)}/
 * 
 * @author Costin Leau
 */
public class JobRunner implements InitializingBean, Callable<Boolean> {

	private static final Log log = LogFactory.getLog(JobRunner.class);

	private boolean runAtStartup = true;
	private boolean waitForJobs = true;
	private Collection<Job> jobs;
	private boolean ignoreFailures = false;

	private Iterable<Callable<?>> preActions;
	private Iterable<Callable<?>> postActions;

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notEmpty(jobs, "at least one job needs to be specified");

		if (runAtStartup) {
			call();
		}
	}

	@Override
	public Boolean call() throws Exception {
		// pre action
		invoke(preActions);

		Boolean succesful = Boolean.TRUE;

		for (Job job : jobs) {
			if (!waitForJobs) {
				job.submit();
			}
			else {
				succesful &= job.waitForCompletion(true);
				if (!ignoreFailures && !succesful) {
					RunningJob rj = JobUtils.getRunningJob(job);
					throw new IllegalStateException("Job [" + job.getJobName() + "] failed - "
							+ (rj != null ? rj.getFailureInfo() : "N/A"));
				}
			}
		}

		// post action
		invoke(postActions);
		return (waitForJobs ? succesful : null);
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
	 * Indicates whether the runner should wait for the jobs to finish (the default) or not.
	 * 
	 * @param waitForJobs The waitForJobs to set.
	 */
	public void setWaitForJobs(boolean waitForJobs) {
		this.waitForJobs = waitForJobs;
	}

	/**
	 * Sets the Jobs to run.
	 * 
	 * @param jobs The jobs to run.
	 */
	public void setJobs(Collection<Job> jobs) {
		this.jobs = jobs;
	}

	/**
	 * Indicates whether job failures are ignored (simply logged) or not (default), meaning
	 * the runner propagates the error further down the stack.
	 * Note this setting applies only if the runner monitors/waits for the jobs.
	 *
	 * @see #setWaitForJobs(boolean)
	 * @param ignoreFailures the new ignore failures
	 */
	public void setIgnoreFailures(boolean ignoreFailures) {
		this.ignoreFailures = ignoreFailures;
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