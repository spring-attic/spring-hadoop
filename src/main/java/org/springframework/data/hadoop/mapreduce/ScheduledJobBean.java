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

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.quartz.QuartzJobBean;

/**
 * Quartz Job wrapper to schedule Hadoop Job. This class is corresponding {@link org.quartz.Job} which
 * actually execute the tasks. Here, it will get all the Hadoop jobs and submit them to Hadoop cluster.
 * 
 * @author Jarred Li
 *
 * @since 1.0
 * @see org.springframework.scheduling.quartz.QuartzJobBean
 * @see HadoopJobDetailBean
 */

public class ScheduledJobBean extends QuartzJobBean {

	private static final Log log = LogFactory.getLog(ScheduledJobBean.class);

	/* (non-Javadoc)
	 * @see org.springframework.scheduling.quartz.QuartzJobBean#executeInternal(org.quartz.JobExecutionContext)
	 */
	@Override
	protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
		JobDataMap dataMap = context.getJobDetail().getJobDataMap();
		@SuppressWarnings("unchecked")
		Set<String> jobNames = (Set<String>)dataMap.get("jobNames");
		boolean waitForJobs = dataMap.getBoolean("waitForJobs");
		ApplicationContext ctx = (ApplicationContext) dataMap.get("applicatonContext");		
		for (String jobName : jobNames) {
			log.info("run job : " + jobName);
			Job job = ctx.getBean(jobName, Job.class);
			try {
				if (!waitForJobs) {
					job.submit();
				}
				else {
					job.waitForCompletion(true);
				}
			} catch (Exception e) {
				log.error("launch scheduled job failed.");
				throw new JobExecutionException("launch scheduled job failed.", e);
			}
		}

	}
}