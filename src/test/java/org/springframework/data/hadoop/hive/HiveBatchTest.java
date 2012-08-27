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
package org.springframework.data.hadoop.hive;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveServerException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.data.hadoop.batch.JobsTrigger;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class HiveBatchTest {

	@Autowired
	private ApplicationContext ctx;

	@Autowired
	private HiveClient client;

	{
		TestUtils.hackHadoopStagingOnWin();
	}

	@Test
	public void testHiveClient() throws Exception {
		ctx.getBean("hiveClient", HiveClient.class);
	}

	@Test
	public void testServerNamespace() throws Exception {
		assertTrue(ctx.isPrototype("hive-script"));
		JobsTrigger tj = new JobsTrigger();
		tj.startJobs(ctx);
	}

	@Test
	public void testTasklet() throws Exception {
		HiveTasklet pt = ctx.getBean("tasklet", HiveTasklet.class);
		pt.execute(null, null);
	}

	@Test(expected = HiveServerException.class)
	public void testScriptRunner() throws Exception {
		HiveScriptRunner.run(client, ctx.getResource("org/springframework/data/hadoop/hive/hive-failing-script.q"));
	}

	@Test
	public void testScriptParams() throws Exception {
		Resource res = new ByteArrayResource("set zzz;set hiveconf:yyy;".getBytes());
		Properties params = new Properties();
		params.put("zzz", "onions");
		params.put("yyy", "unleashed");

		List<String> run = HiveScriptRunner.run(client, res, params);
		assertEquals("zzz=onions", run.get(0));
		assertEquals("hiveconf:yyy=unleashed", run.get(1));
	}
}