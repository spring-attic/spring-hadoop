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

import org.apache.hadoop.mapreduce.Job;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.hadoop.HadoopException;
import org.springframework.util.Assert;

/**
 * Batch tasklet for executing one Hadoop job.
 * Can be configured to not wait for the job to finish - by default the tasklet waits for the job submitted to finish.
 * 
 * @author Costin Leau
 */
public class JobTasklet implements InitializingBean, Tasklet {

	private Job job;
	private boolean waitForJob = true;

	public void afterPropertiesSet() {
		Assert.notNull(job, "A Hadoop job is required");
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		Exception exc = null;

		try {
			if (!waitForJob) {
				job.submit();
			}
			else {
				job.waitForCompletion(false);
			}
			return RepeatStatus.FINISHED;
		} catch (Exception ex) {
			exc = ex;
		}
		String message = "Job [" + job.getJobID() + "|" + job.getJobName() + " ] failed";
		throw (exc != null ? new HadoopException(message, exc) : new HadoopException(message));
	}

	/**
	 * @param job The job to set.
	 */
	public void setJob(Job job) {
		this.job = job;
	}

	public void setWaitForJob(boolean waitForJob) {
		this.waitForJob = waitForJob;
	}
}