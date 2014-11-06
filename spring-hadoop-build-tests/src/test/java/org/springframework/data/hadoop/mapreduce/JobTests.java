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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.hadoop.mapreduce.ToolTests.TestTool;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class JobTests {

	public static class JobInfo {
		public JobID jobId;
		public org.apache.hadoop.mapred.JobID oldJobId;
		public RunningJob runningJob;
		public JobConf jobConf;

		public void setJobId(JobID jobId) {
			this.jobId = jobId;
		}

		public void setOldJobId(org.apache.hadoop.mapred.JobID oldJobId) {
			this.oldJobId = oldJobId;
		}

		public void setRunningJob(RunningJob runningJob) {
			this.runningJob = runningJob;
		}

		public void setJobConf(JobConf jobConf) {
			this.jobConf = jobConf;
		}
	}

	@Autowired
	private ApplicationContext ctx;
	@Resource(name = "ns-job")
	private Job job;

	private JobInfo jobInfo;

	@Before
	public void before() {
		TestTool.conf = null;
		TestTool.obj = null;
		TestTool.args = null;
	}

	@Test
	public void testSanityTest() throws Exception {
		assertNotNull(ctx);
	}

	@Test
	public void testJarJob() throws Exception {
		Job job = ctx.getBean("jar-job", Job.class);
		assertTrue(ctx.isPrototype("jar-job"));
		assertNotNull(job.getJar());
		assertEquals("true", job.getConfiguration().get("mapred.used.genericoptionsparser"));
	}

	@Test
	public void testCustomJarJob() throws Exception {
		Job job = ctx.getBean("custom-jar-job", Job.class);
		assertTrue(ctx.isPrototype("custom-jar-job"));
		assertNotNull(job.getJar());
		assertEquals("true", job.getConfiguration().get("mapred.used.genericoptionsparser"));

		ClassLoader loader = job.getConfiguration().getClassLoader();
		assertFalse(Thread.currentThread().getContextClassLoader().equals(loader));
	}

	@Test
	public void testJobProperties() throws Exception {
		assertNotNull(job);
		Configuration cfg = job.getConfiguration();
		assertNotNull(cfg);

		assertEquals("chasing", cfg.get("star"));
		assertEquals("captain eo", cfg.get("return"));
		assertEquals("last", cfg.get("train"));
		assertEquals("the dream", cfg.get("dancing"));
		assertEquals("in the mirror", cfg.get("tears"));
		assertEquals("eo", cfg.get("captain"));
		assertEquals("8", cfg.get("mapred.reduce.tasks"));
		// will always be 1 when local
		//assertEquals("4", cfg.get("mapred.map.tasks"));
		System.out.println(cfg.get("mapred.map.tasks"));

		assertEquals("true", job.getConfiguration().get("mapred.used.genericoptionsparser"));
		//System.in.read();
	}

	public void testPropertyEditors() throws Exception {
		assertNotNull(jobInfo);
		assertNotNull(jobInfo.jobConf);
		assertNotNull(jobInfo.jobId);
		assertNotNull(jobInfo.oldJobId);
		assertNotNull(jobInfo.runningJob);
	}

}