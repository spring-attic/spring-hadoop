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

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.hadoop.hive.service.HiveClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.data.hadoop.batch.JobsTrigger;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ReflectionUtils;

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
	private ObjectFactory<HiveClient> factory;
	@Autowired
	private HiveOperations template;

	{
		TestUtils.hackHadoopStagingOnWin();
	}
	
	@Test
	public void testHiveClient() throws Exception {
		ctx.getBean("hiveClient", ObjectFactory.class);
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

	@Test
	public void testScriptNSParams() throws Exception {
		HiveTasklet pt = ctx.getBean("hive-script", HiveTasklet.class);

		Field findField = ReflectionUtils.findField(HiveTasklet.class, "scripts");
		ReflectionUtils.makeAccessible(findField);

		Iterator<HiveScript> scripts = ((Collection<HiveScript>) ReflectionUtils.getField(findField, pt)).iterator();
		scripts.next();

		Iterator<String> keys = scripts.next().getArguments().keySet().iterator();
		assertEquals("war", keys.next());
		assertEquals("blue", keys.next());
		assertEquals("white", keys.next());

		pt.execute(null, null);
	}

	@Test(expected = Exception.class)
	public void testScriptRunner() throws Exception {
		template.query("org/springframework/data/hadoop/hive/hive-failing-script.q");
	}

	@Test
	public void testTemplateWithScript() throws Exception {
		//Integer in = template.queryForInt("classpath:org/springframework/data/hadoop/hive/script.q");
		assertTrue(!template.query("classpath:org/springframework/data/hadoop/hive/script.q").isEmpty());
	}

	@Test
	public void testQueryForInt() throws Exception {
		assertEquals(Integer.valueOf(0), template.queryForInt("DROP TABLE IF EXISTS testHiveBatchTable; " +
				"create table testHiveBatchTable (key int, value string);" +
				"select count(1) from testHiveBatchTable;"));
	}

	@Test
	public void testScriptParams() throws Exception {
		Resource res = new ByteArrayResource("set zzz;set hiveconf:yyy;".getBytes());
		Properties params = new Properties();
		params.put("zzz", "onions");
		params.put("yyy", "unleashed");

		List<String> run = template.executeScript(new HiveScript(res, params));
		assertEquals("zzz=onions", run.get(0));
		assertEquals("hiveconf:yyy=unleashed", run.get(1));
	}

	@Test
	public void testHiveRunner() throws Exception {
		Callable runner = ctx.getBean("hive-scripts", Callable.class);
		System.out.println(runner.call());
	}
}