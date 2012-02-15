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

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.data.hadoop.mapreduce.ToolTests.TestTool;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class JobTests {

	@Autowired
	private ApplicationContext ctx;
	@Resource(name = "ns-job")
	private Job job;

	{
		TestUtils.hackHadoopStagingOnWin();
	}

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
		assertNotNull(job.getJar());
		assertEquals("true", job.getConfiguration().get("mapred.used.genericoptionsparser"));
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

		assertEquals("true", job.getConfiguration().get("mapred.used.genericoptionsparser"));
	}

}