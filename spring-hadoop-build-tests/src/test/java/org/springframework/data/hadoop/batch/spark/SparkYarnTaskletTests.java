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
package org.springframework.data.hadoop.batch.spark;

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
 * This test only runs if you specify 'enableSparkTests' during the build.
 *
 * It requires that you copy the have Spark Assembly jar (spark-assembly-1.4.1-hadoop2.6.0.jar)
 * to HDFS in a directory named /app/spark/
 *
 * It also requires that you copy the spring-data-hadoop-spark-{version}-tests.jar to
 * spring-hadoop-build-tests/src/test/resources/spark-tests.jar
 * (only necessary if you change the test code)
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/org/springframework/data/hadoop/batch/spark/spark-batch.xml")
public class SparkYarnTaskletTests {

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
		StepExecution sparkStep = null;
		for (StepExecution se : stepExecutions) {
			if ("spark".equals(se.getStepName())) {
				sparkStep = se;
			}
		}
		assertNotNull(sparkStep);
		assertEquals(ExitStatus.COMPLETED, sparkStep.getExitStatus());
		ctx.close();
	}

}