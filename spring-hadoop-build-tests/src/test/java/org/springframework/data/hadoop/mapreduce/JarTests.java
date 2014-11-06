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

import java.util.Enumeration;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.hadoop.batch.JobsTrigger;
import org.springframework.data.hadoop.mapreduce.ExecutionUtils.ExitTrapped;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ClassUtils;

import static org.junit.Assert.*;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
public class JarTests {

	@Autowired
	private ApplicationContext ctx;
	@Autowired
	private Configuration hadoopConfiguration;


	@AfterClass
	public static void cleanProps() {
		Properties properties = System.getProperties();
		Enumeration<?> props = properties.propertyNames();

		while (props.hasMoreElements()) {
			Object key = props.nextElement();
			Object value = properties.get(key);
			if (!value.getClass().equals(String.class)) {
				System.out.println("Removing incorrect key [" + key + "] w/ value " + value);
				properties.remove(key);
			}
		}
	}

	@Test
	public void testTasklet() throws Exception {
		JobsTrigger.startJobs(ctx);
	}

	@Test
	public void testTaskletScope() throws Exception {
		assertTrue(ctx.isPrototype("tasklet-ns"));
	}

	@Test
	public void testBadMainClassLoaded() throws Exception {
		assertNotNull(System.getProperties().getProperty("org.springframework.data.jar.init"));
	}

	@Test
	public void testBadMainClassConfiguration() throws Exception {
		Configuration cfg = (Configuration) System.getProperties().get("org.springframework.data.hadoop.jar.cfg");
		assertNotNull(cfg);
		cfg.getClassLoader();

		// check inherited props
		assertEquals("main", hadoopConfiguration.get("cfg"));

		assertEquals("main", cfg.get("cfg"));
		// check nested props
		assertEquals("war", cfg.get("web"));
		assertNull(cfg.get("land"));

		// verify new config object
		Configuration freshConfig = new Configuration();

		// check inherited props
		assertNull(freshConfig.get("cfg"));
		// check nested props
		assertNull(freshConfig.get("web"));
		assertNull(freshConfig.get("land"));
	}


	@Test
	public void testClassVisibility() throws Exception {
		ClassLoader loader = ctx.getClassLoader();
		assertFalse(ClassUtils.isPresent("test.MainClass", loader));
		assertFalse(ClassUtils.isPresent("test.OtherMainClass", loader));
	}


	public void testBadMainClassReturnCode() throws Exception {
		assertEquals(Integer.valueOf(1), ctx.getBean("bad-main-class"));
	}

	@Test
	public void testBadMainClassArgs() throws Exception {
		String args[] = (String[]) System.getProperties().get("org.springframework.data.hadoop.jar.args");
		assertNotNull(args);
		assertEquals(1, args.length);
		assertEquals("bad", args[0]);
	}

	@Test
	public void testBadMainClassPreExit() throws Exception {
		assertNotNull(System.getProperties().get("org.springframework.data.jar.exit.pre"));
	}

	@Test
	public void testExitTrap() throws Exception {
		Error trap = (Error) System.getProperties().get("org.springframework.data.jar.exit.exception");
		assertNotNull(trap);
		assertEquals(ExitTrapped.class, trap.getClass());
		assertEquals(1, ((ExitTrapped) trap).getExitCode());
	}

	@Test
	public void testOtherMainClassLoaded() throws Exception {
		assertNotNull(System.getProperties().getProperty("org.springframework.data.jar.init.other"));
	}

	public void testOtherMainClassReturnCode() throws Exception {
		assertEquals(Integer.valueOf(42), ctx.getBean("other-class"));
	}

	@Test
	public void testOtherMainClassConfiguration() throws Exception {
		Configuration cfg = (Configuration) System.getProperties().get("org.springframework.data.hadoop.jar.other.cfg");
		assertNotNull(cfg);
		// check inherited props
		assertEquals("main", hadoopConfiguration.get("cfg"));
		assertEquals("main", cfg.get("cfg"));
		// check nested props
		assertNull(cfg.get("web"));
		assertEquals("buckethead", cfg.get("land"));

		// verify new config object
		Configuration freshConfig = new Configuration();

		// check inherited props
		assertNull(freshConfig.get("cfg"));
		// check nested props
		assertNull(freshConfig.get("web"));
		assertNull(freshConfig.get("land"));
	}

	@Test
	public void testOtherMainClassArgs() throws Exception {
		String args[] = (String[]) System.getProperties().get("org.springframework.data.hadoop.jar.other.args");
		assertNotNull(args);
		assertEquals(1, args.length);
		assertEquals("42", args[0]);
	}

}