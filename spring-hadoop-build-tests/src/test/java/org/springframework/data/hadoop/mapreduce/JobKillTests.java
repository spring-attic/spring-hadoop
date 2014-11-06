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

import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.hadoop.batch.JobsTrigger;
import org.springframework.data.hadoop.mapreduce.JobUtils.JobStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class JobKillTests {

	@Autowired
	private ApplicationContext ctx;

	private static long WAIT_FOR_JOB_TO_START = 1 * 1000;

	@Test
	public void testJobKill() throws Exception {
		Job victimJob = ctx.getBean("victim-job", Job.class);
		JobRunner runner = ctx.getBean("killer-runner", JobRunner.class);

		assertFalse(JobUtils.getStatus(victimJob).isStarted());
		runner.call();

		// wait a bit for the job to be started
		while (!(JobUtils.getStatus(victimJob).isRunning() || JobUtils.getStatus(victimJob).isFinished())) {
			Thread.sleep(WAIT_FOR_JOB_TO_START);
		}
		runner.destroy();

		checkHadoopJobWasKilled(victimJob);
	}

	@Test
	public void testJobTaskletKill() throws Exception {
		Job victimJob = ctx.getBean("tasklet-victim-job", Job.class);

		// start async job execution
		JobExecution batchJob = JobsTrigger.startJob(ctx, "mainJob");

		// wait a bit for the job to be started
		while (!(JobUtils.getStatus(victimJob).isRunning() || JobUtils.getStatus(victimJob).isFinished())) {
			Thread.sleep(WAIT_FOR_JOB_TO_START);
		}

		batchJob.stop();

		checkHadoopJobWasKilled(victimJob);
	}

	private static void checkHadoopJobWasKilled(Job victimJob) throws Exception {
		JobStatus status = JobStatus.UNKNOWN;
		// wait for the job status to be updated...
		for (int i = 0; i < 5 && !status.isFinished(); i++) {
			Thread.sleep(1000 * 5);
			status = JobUtils.getStatus(victimJob);
		}
		JobStatus status2 = JobUtils.getStatus(victimJob);
		assertTrue("job not killed - " + status2, (JobStatus.KILLED == status2 || JobStatus.FAILED == status2));
		assertTrue(status.isFinished());
	}
}