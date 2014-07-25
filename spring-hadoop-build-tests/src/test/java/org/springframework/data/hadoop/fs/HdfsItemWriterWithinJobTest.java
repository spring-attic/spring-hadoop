/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.hadoop.fs;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author liujiong
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = HadoopDelegatingSmartContextLoader.class, locations = { "/org/springframework/data/hadoop/fs/HdfsItemWriterTest-context.xml" })
@MiniHadoopCluster
public class HdfsItemWriterWithinJobTest {


	@Autowired
	ApplicationContext context;

	@Test
	public void testWithinJob() throws Exception {
		JobLauncher launcher = context.getBean(JobLauncher.class);
		Job job = context.getBean(Job.class);

		JobParameters jobParameters = new JobParametersBuilder().toJobParameters();

		JobExecution execution = launcher.run(job, jobParameters);
		assertTrue("status was: " + execution.getStatus(), execution.getStatus() == BatchStatus.COMPLETED);
	}

}
