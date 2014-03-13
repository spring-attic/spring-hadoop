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
package org.springframework.data.hadoop.batch.mapreduce;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.hadoop.mapreduce.JobExecutor;
import org.springframework.data.hadoop.mapreduce.JobUtils;

/**
 * Batch tasklet for executing one Hadoop job.
 * Can be configured to not wait for the job to finish - by default the tasklet waits for the job submitted to finish.
 *
 * @author Costin Leau
 * @author Thomas Risberg
 */
public class JobTasklet extends JobExecutor implements Tasklet {


	public RepeatStatus execute(final StepContribution contribution, ChunkContext chunkContext) throws Exception {

		StepContext context = StepSynchronizationManager.getContext();
		final StepExecution stepExecution = (context != null) ? context.getStepExecution() : null;

		final AtomicBoolean done = new AtomicBoolean(false);

		final JobListener jobListener = new JobListener() {

			@Override
			public Object beforeAction() {
				// double check the underlying thread to see whether it is aware of a step execution
				if (StepSynchronizationManager.getContext() == null) {
					StepSynchronizationManager.register(stepExecution);
					return Boolean.TRUE;
				}
				return Boolean.FALSE;
			}

			@Override
			public void afterAction(Object state) {
				if (Boolean.TRUE.equals(state)) {
					StepSynchronizationManager.close();
				}
				done.set(true);
				synchronized (done) {
					done.notify();
				}
			}

			@Override
			public void jobKilled(Job job) {
				saveCounters(job, contribution);
				saveJobStats(job, stepExecution);
			}

			@Override
			public void jobFinished(Job job) {
				saveCounters(job, contribution);
				saveJobStats(job, stepExecution);
			}
		};

		startJobs(jobListener);

		boolean stopped = false;
		// check status (if we have to wait)
		if (isWaitForCompletion()) {
			while (!done.get() && !stopped) {
				if (stepExecution.isTerminateOnly()) {
					log.info("Cancelling job tasklet");
					stopped = true;
					stopJobs(jobListener);

					// wait for stopping to properly occur
					while (!done.get()) {
						synchronized (done) {
							done.wait();
						}
					}
				}
				else {
					// wait a bit more then the internal hadoop threads
					Thread.sleep(5500);
				}
			}
		}

		return RepeatStatus.FINISHED;
	}

	@SuppressWarnings("deprecation")
	private void saveCounters(Job job, StepContribution contribution) {
		Counters counters = null;
		try {
			counters = job.getCounters();
		} catch (Exception ex) {
			if (RuntimeException.class.isAssignableFrom(ex.getClass())) {
				throw (RuntimeException)ex;
			} else {
				// ignore - we just can't get stats
			}
		}
		if (counters == null) {
			return;
		}

		// TODO: remove deprecation suppress when we don't want to rely on org.apache.hadoop.mapred
		Counter count = counters.findCounter(Task.Counter.MAP_INPUT_RECORDS);

		for (int i = 0; i < safeLongToInt(count.getValue()); i++) {
			contribution.incrementReadCount();
		}

		count = counters.findCounter(Task.Counter.MAP_SKIPPED_RECORDS);
		contribution.incrementReadSkipCount(safeLongToInt(count.getValue()));

		count = counters.findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS);
		contribution.incrementWriteCount(safeLongToInt(count.getValue()));

		count = counters.findCounter(Task.Counter.REDUCE_SKIPPED_RECORDS);

		for (int i = 0; i < safeLongToInt(count.getValue()); i++) {
			contribution.incrementWriteSkipCount();
		}
	}

	private static void saveJobStats(Job job, StepExecution stepExecution) {
		if (stepExecution == null) {
			return;
		}
		ExecutionContext executionContext = stepExecution.getExecutionContext();
		String statusPrefix = "Job Status::";
		executionContext.put(statusPrefix + "ID", JobUtils.getJobId(job).toString());
		executionContext.put(statusPrefix + "Name", job.getJobName());
		executionContext.put(statusPrefix + "Tracking URL", job.getTrackingURL());
		executionContext.put(statusPrefix + "State", JobUtils.getStatus(job).toString());
		try {
			for (String cgName : job.getCounters().getGroupNames()) {
				CounterGroup group = job.getCounters().getGroup(cgName);
				Iterator<Counter> ci = group.iterator();
				while (ci.hasNext()) {
					Counter c = ci.next();
					executionContext.put(group.getDisplayName().trim() + "::" + c.getDisplayName().trim(), c.getValue());
				}
			}
		} catch (Exception ignore) {}
	}

	static int safeLongToInt(long l) {
		if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
			throw new IllegalArgumentException(l + " cannot be cast to int without changing its value.");
		}
		return (int) l;
	}
}