/*
 * Copyright 2011 the original author or authors.
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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

/**
 * Batch tasklet for executing one Hadoop job.
 * Can be configured to not wait for the job to finish - by default the tasklet waits for the job submitted to finish.
 * 
 * @author Costin Leau
 */
public class JobTasklet extends JobExecutor implements Tasklet {


	public RepeatStatus execute(final StepContribution contribution, ChunkContext chunkContext) throws Exception {

		StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();

		final AtomicBoolean done = new AtomicBoolean(false);

		final JobListener jobListener = new JobListener() {
			@Override
			public void jobKilled(Job job) {
				done.set(true);
				saveCounters(job, contribution);
			}

			@Override
			public void jobFinished(Job job) {
				done.set(true);
				saveCounters(job, contribution);
			}
		};

		startJobs(jobListener);

		boolean stopped = false;
		// check status (if we have to wait)
		if (isWaitForJob()) {
			while (!done.get() && !stopped) {
				if (stepExecution.isTerminateOnly()) {
					stopped = true;
					stopJobs(jobListener);
				}
				else {
					// wait a bit more then the internal hadoop threads
					Thread.sleep(5500);
				}
			}
		}

		return RepeatStatus.FINISHED;
	}

	private void saveCounters(Job job, StepContribution contribution) {
		Counters counters = null;
		try {
			counters = job.getCounters();
		} catch (IOException ex) {
			// ignore - we just can't get stats
		}
		if (counters == null) {
			return;
		}

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

	static int safeLongToInt(long l) {
		if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
			throw new IllegalArgumentException(l + " cannot be cast to int without changing its value.");
		}
		return (int) l;
	}
}