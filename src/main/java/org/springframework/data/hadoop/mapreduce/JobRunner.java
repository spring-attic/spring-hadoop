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

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.Assert;

/**
 * Simple runner for submitting Hadoop jobs sequentially. By default, the runner waits for the jobs to finish and returns a boolean indicating
 * whether all the jobs succeeded or not (when there's no waiting, the status cannot be determined and null is returned).
 * <p/>
 * For more control over the job execution and outcome consider querying the {@link Job}s or using Spring Batch (see the reference documentation for more info). 
 * 
 * @author Costin Leau
 */
public class JobRunner implements FactoryBean<Object>, InitializingBean, DisposableBean, ApplicationContextAware {

	private static final Log log = LogFactory.getLog(JobRunner.class);

	private boolean runAtStartup = true;
	private boolean waitForJobs = true;
	private Set<String> jobNames;
	private Set<Job> jobs = new HashSet<Job>();
	//private boolean executed = false;
	//private boolean succesful = false;
	
	//private String jobBeanName;
	private ApplicationContext ctx;

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notEmpty(jobNames, "at least one job needs to be specified");

		if (runAtStartup) {
			runJob();
		}
	}

	@Override
	public void destroy() throws Exception {
		if (!waitForJobs) {
			for (Job job : jobs) {
				try {
					job.killJob();
				} catch (Exception ex) {
					log.warn("Cannot kill job [" + job.getJobID() + "|" + job.getJobName() + " ] failed", ex);
				}
			}
		}
	}

	@Override
	public Object getObject() throws Exception {
		//TODO: figure out what should be returned
		return jobNames.size();
	}

	@Override
	public Class<?> getObjectType() {
		return Boolean.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}


	/**
	 * Indicates whether the jobs should be submitted at startup or not.
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
	public void setJobNames(Set<String> names) {
		this.jobNames = names;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.ctx = applicationContext;
		
	}
	
	/**
	 * Run all the jobs.
	 * 
	 * @throws Exception 
	 */
	public void runJob() throws Exception{
		for(String jobName : jobNames){
			log.info("run job : " + jobName);
			Job job = ctx.getBean(jobName, Job.class);
			jobs.add(job);
			if(!this.waitForJobs){
				job.submit();
			}
			else{
				job.waitForCompletion(true);
			}
		}		
	}


}
