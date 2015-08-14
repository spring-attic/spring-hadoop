/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.hadoop.batch.sqoop2;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Thomas Risberg
 *
 * This test only runs if you specify 'enableSqoop2Tests' during the build. 
 * It requires that you have Sqoop2 installed and running on the default port 12000 on localhost. 
 * You also need a job with ID 1 defined.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/org/springframework/data/hadoop/batch/sqoop2/sqoop2-batch.xml")
public class Sqoop2TaskletTests {

	@Autowired
	private AbstractApplicationContext ctx;

	@Test
	public void testTasklet() throws Exception {
		JobLauncher jobLauncher = ctx.getBean(JobLauncher.class);
		Job job = ctx.getBean(Job.class);
		JobExecution jobExecution = jobLauncher.run(
				job,
				new JobParametersBuilder().toJobParameters());
		Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
		StepExecution sqoop2Step = null;
		for (StepExecution se : stepExecutions) {
			if ("sqoop2".equals(se.getStepName())) {
				sqoop2Step = se;
			}
		}
		assertNotNull(sqoop2Step);
		assertEquals(ExitStatus.COMPLETED, sqoop2Step.getExitStatus());
		assertEquals("1", sqoop2Step.getExecutionContext().get("sqoop2.job.id"));
		assertTrue(sqoop2Step.getExecutionContext().get("sqoop2.external.job.id").toString().length() > 10);
		assertEquals("SUCCEEDED", sqoop2Step.getExecutionContext().get("sqoop2.job.status"));
		assertTrue(sqoop2Step.getExecutionContext().get("sqoop2.job.counters").toString().length() > 100);
		ctx.close();
	}

}