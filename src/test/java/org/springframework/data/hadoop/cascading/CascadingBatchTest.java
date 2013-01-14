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
package org.springframework.data.hadoop.cascading;

import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.data.hadoop.batch.JobsTrigger;

import cascading.cascade.Cascade;
import cascading.stats.CascadeStats;
import static org.junit.Assert.assertTrue;

/**
 * @author Costin Leau
 */
public class CascadingBatchTest {

	{
		TestUtils.hackHadoopStagingOnWin();
	}

	private static long WAIT_FOR_JOB = 1 * 1000;

	@Autowired
	GenericXmlApplicationContext ctx;

	@Before
	public void setup() {
		ctx = new GenericXmlApplicationContext(
				"/org/springframework/data/hadoop/cascading/CascadingBatchTest-context.xml");

		ctx.registerShutdownHook();
	}

	@After
	public void cleanup() {
		ctx.close();
	}

	@Test
	public void testCascadeTasklet() throws Exception {
		JobExecution batchJob = JobsTrigger.startJob(ctx, "mainJob");

		// check records
		Collection<StepExecution> steps = batchJob.getStepExecutions();
		for (StepExecution stepExecution : steps) {
			if ("do-cascade".equals(stepExecution.getStepName())) {
				assertTrue(stepExecution.getReadCount() > 0);
			}
		}
	}

	@Test
	public void testJobTaskletKill() throws Exception {
		// start async job execution
		JobExecution batchJob = JobsTrigger.startJob(ctx, "mainJob");
		Cascade cascade = ctx.getBean("cascade", Cascade.class);

		while (!cascade.getStats().isEngaged()) {
			Thread.sleep(WAIT_FOR_JOB);
		}

		batchJob.stop();
		CascadeStats stats = cascade.getStats();
		while (!stats.isFinished()) {
			Thread.sleep(WAIT_FOR_JOB);
		}
		assertTrue(stats.isStopped());
	}
}
