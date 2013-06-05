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
package org.springframework.data.hadoop.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.ListableBeanFactory;

/**
 * Trigger bean used for executing jobs after a context has been initialized.
 * 
 * @author Costin Leau
 */
public class JobsTrigger {

	public static List<JobExecution> startJobs(ListableBeanFactory bf) {
		return startJobs(bf, new JobParameters());
	}

	public static List<JobExecution> startJobs(ListableBeanFactory bf, JobParameters params) {
		JobLauncher launcher = bf.getBean(JobLauncher.class);
		Map<String, Job> jobs = bf.getBeansOfType(Job.class);

		List<JobExecution> executions = new ArrayList<JobExecution>(jobs.size());
		for (Map.Entry<String, Job> entry : jobs.entrySet()) {
			RuntimeException e = null;
			try {
				JobExecution jobExec = launcher.run(entry.getValue(), params);
				executions.add(jobExec);
				if (jobExec.getStatus().equals(BatchStatus.FAILED)) {
					e = new BeanInitializationException("Failed executing job " + entry.getKey());
				}
			} catch (Exception ex) {
				throw new BeanInitializationException("Cannot execute job " + entry.getKey(), ex);
			}
			if (e != null) {
				throw e;
			}
		}
		return executions;
	}

	public static JobExecution startJob(ListableBeanFactory lbf, String jobName) throws Exception {
		Job job = lbf.getBean(jobName, Job.class);
		JobLauncher launcher = lbf.getBean(JobLauncher.class);
		return launcher.run(job, new JobParameters());
	}
}
