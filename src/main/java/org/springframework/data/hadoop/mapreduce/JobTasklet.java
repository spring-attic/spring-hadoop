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

import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.hadoop.HadoopException;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Batch tasklet for executing one Hadoop job.
 * Can be configured to not wait for the job to finish - by default the tasklet waits for the job submitted to finish.
 * 
 * @author Costin Leau
 */
public class JobTasklet implements InitializingBean, Tasklet, BeanFactoryAware {

	private Job job;
	private String jobName;
	private boolean waitForJob = true;
	private BeanFactory beanFactory;
	private boolean verbose = true;

	public void afterPropertiesSet() {
		Assert.isTrue(job != null || StringUtils.hasText(jobName), "A Hadoop job or its name is required");

		if (StringUtils.hasText(jobName)) {
			Assert.notNull(beanFactory, "a bean factory is required if the job is specified by name");
			Assert.isTrue(beanFactory.containsBean(jobName), "beanFactory does not contain any bean named [" + jobName + "]");
		}
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		Exception exc = null;

		Job j = (job != null ? job : beanFactory.getBean(jobName, Job.class));

		try {
			if (!waitForJob) {
				j.submit();
			}
			else {
				j.waitForCompletion(verbose);
			}
			return RepeatStatus.FINISHED;
		} catch (Exception ex) {
			exc = ex;
		}
		RunningJob rj = JobUtils.getRunningJob(job);
		String message = "Job [" + j.getJobID() + "|" + j.getJobName() + " ] failed - "
				+ (rj != null ? rj.getFailureInfo() : "N/A");
		throw (exc != null ? new HadoopException(message, exc) : new HadoopException(message));
	}

	/**
	 * Sets the job to execute.
	 * 
	 * @param job The job to execute.
	 */
	public void setJob(Job job) {
		this.job = job;
	}

	/**
	 * Sets the job to execute by (bean) name. This is the default
	 * method used by the hdp name space to allow lazy initialization and potential scoping
	 * to kick in.
	 * 
	 * @param jobName The job to execute.
	 */
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	/**
	 * Indicates whether the tasklet should return for the job to complete (default)
	 * after submission or not.
	 * 
	 * @param waitForJob whether to wait for the job to complete or not.
	 */
	public void setWaitForJob(boolean waitForJob) {
		this.waitForJob = waitForJob;
	}

	/**
	 * Indicates whether the job execution is verbose (the default) or not.
	 * 
	 * @param verbose whether the job execution is verbose or not.
	 */
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}
}