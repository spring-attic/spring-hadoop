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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Common class shared for executing Hadoop {@link Job}s.
 * 
 * @author Costin Leau
 */
abstract class JobExecutor implements InitializingBean, BeanFactoryAware {

	private Iterable<Job> jobs;
	private Iterable<String> jobNames;
	private boolean waitForJobs = true;
	private BeanFactory beanFactory;
	private boolean verbose = true;

	public void afterPropertiesSet() throws Exception {
		Assert.isTrue(jobs != null | jobNames != null, "A Hadoop job or its name is required");

		if (jobNames != null) {
			for (String jobName : jobNames) {
				if (StringUtils.hasText(jobName)) {
					Assert.notNull(beanFactory, "a bean factory is required if the job is specified by name");
					Assert.isTrue(beanFactory.containsBean(jobName), "beanFactory does not contain any bean named ["
							+ jobNames + "]");
				}
			}
		}
	}

	protected void executeJobs() throws Exception {
		Boolean succesful = Boolean.TRUE;

		Iterable<Job> jbs = findJobs();

		for (Job job : jbs) {
			if (!waitForJobs) {
				job.submit();
			}
			else {
				succesful &= job.waitForCompletion(verbose);
				if (!succesful) {
					RunningJob rj = JobUtils.getRunningJob(job);
					throw new IllegalStateException("Job [" + job.getJobName() + "] failed - "
							+ (rj != null ? rj.getFailureInfo() : "N/A"));
				}
			}
		}
	}

	private Iterable<Job> findJobs() {
		if (jobs != null) {
			return jobs;
		}
		List<Job> jobs = new ArrayList<Job>();
		for (String name : jobNames) {
			jobs.add(beanFactory.getBean(name, Job.class));
		}
		return jobs;
	}

	/**
	 * Sets the job to execute.
	 * 
	 * @param job The job to execute.
	 */
	public void setJob(Job job) {
		this.jobs = Collections.singleton(job);
	}

	/**
	 * Sets the jobs to execute.
	 * 
	 * @param jobs The job to execute.
	 */
	public void setJobs(Iterable<Job> jobs) {
		this.jobs = jobs;
	}

	/**
	 * Sets the jobs to execute by (bean) name. This is the default
	 * method used by the hdp name space to allow lazy initialization and potential scoping
	 * to kick in.
	 * 
	 * @param jobName The job to execute.
	 */
	public void setJobNames(String... jobName) {
		this.jobNames = Arrays.asList(jobName);
	}

	/**
	 * Indicates whether the tasklet should return for the job to complete (default)
	 * after submission or not.
	 * 
	 * @param waitForJob whether to wait for the job to complete or not.
	 */
	public void setWaitForJob(boolean waitForJob) {
		this.waitForJobs = waitForJob;
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