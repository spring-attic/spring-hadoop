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

import java.net.URL;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.hadoop.batch.JobsTrigger;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class ToolTests {

	public static class TestTool implements Tool {

		public static Configuration conf;
		public static Object obj;
		public static String[] args;

		public TestTool() {

		}

		public TestTool(Object obj) {

		}

		@Override
		public void setConf(Configuration conf) {
			TestTool.conf = conf;
		}

		@Override
		public Configuration getConf() {
			return conf;
		}

		@Override
		public int run(String[] args) throws Exception {
			TestTool.args = args;
			return 0;
		}
	}

	@Autowired
	private ApplicationContext ctx;

	@Test
	public void testToolClass() throws Exception {
		ctx.getBean("simple");
		assertNotNull(TestTool.conf);
		assertEquals(0, TestTool.args.length);
	}

	@Test
	public void testToolConfiguration() throws Exception {
		ctx.getBean("ref");
		assertNotNull(TestTool.conf);
		assertEquals(0, TestTool.args.length);
	}

	@Test
	public void testToolArgs() throws Exception {
		Callable<?> runner = ctx.getBean("nested", Callable.class);
		runner.call();
		assertNotNull(TestTool.conf);
		Configuration conf = TestTool.conf;
		assertFalse(ctx.getBean("hadoopConfiguration").equals(conf));

		assertEquals("for santa", conf.get("cookies"));

		String[] args = TestTool.args;
		assertNotNull(args);
		assertEquals(3, args.length);
		assertEquals("--local", args[0]);
		assertEquals("data/in.txt", args[1]);
		assertEquals("data/out.txt", args[2]);
	}

	@Test
	public void testTaskletScope() throws Exception {
		assertTrue(ctx.isPrototype("tasklet-ns"));
	}

	@SuppressWarnings("static-access")
	@Test
	public void testTasklet() throws Exception {
		JobsTrigger tj = new JobsTrigger();
		tj.startJobs(ctx);
	}

	@Test
	public void testToolJarLoading() throws Exception {
		ClassLoader loader = getClass().getClassLoader();
		Callable<?> runner = ctx.getBean("tool-jar", Callable.class);
		Object result = runner.call();

		assertNotNull(System.getProperty("org.springframework.data.tool.init"));
		assertEquals(Integer.valueOf(13), result);
		assertFalse(org.springframework.util.ClassUtils.isPresent("test.SomeTool", loader));

		@SuppressWarnings("resource")
		ParentLastURLClassLoader cl = new ParentLastURLClassLoader(
				new URL[] { ctx.getResource("some-tool.jar").getURL() }, loader);

		System.out.println("Loading classes ...");
		cl.loadClass("test.inner.InnerToolClass");
		cl = null;

//		System.out.println("****************** Forcing GC **************************");
		System.gc();
		System.gc();
		Thread.sleep(1 * 1000);
		System.gc();
		Thread.sleep(1 * 1000);
//		System.in.read();
	}
}