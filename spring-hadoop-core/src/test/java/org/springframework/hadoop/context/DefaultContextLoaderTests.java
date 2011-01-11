/*
 * Copyright 2006-2010 the original author or authors.
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

package org.springframework.hadoop.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.util.ClassUtils;

/**
 * @author Dave Syer
 * 
 */
public class DefaultContextLoaderTests {

	private DefaultContextLoader loader = new DefaultContextLoader();

	private Configuration configuration = new Configuration();

	@Before
	public void setUp() {
		configuration.setClass(DefaultContextLoader.SPRING_CONFIG_LOCATION, TestConfiguration.class, Object.class);
	}

	@Test
	public void testGetBeanFromNewContext() {
		TestContextHolder holder = loader.getBean(configuration, TestContextHolder.class, true);
		assertTrue(holder.getApplicationContext().containsBean(DefaultContextLoader.SPRING_CONFIG_BOOTSTRAP));
		Properties properties = holder.getApplicationContext().getBean(DefaultContextLoader.SPRING_CONFIG_BOOTSTRAP,
				Properties.class);
		assertEquals("{}", properties.toString());
	}

	@Test
	public void testGetBeanFromNewContextWithBootstrap() {
		configuration.set(DefaultContextLoader.SPRING_CONFIG_BOOTSTRAP, "foo=bar");
		TestContextHolder holder = loader.getBean(configuration, TestContextHolder.class, true);
		assertTrue(holder.getApplicationContext().containsBean(DefaultContextLoader.SPRING_CONFIG_BOOTSTRAP));
		Properties properties = holder.getApplicationContext().getBean(DefaultContextLoader.SPRING_CONFIG_BOOTSTRAP,
				Properties.class);
		assertEquals("{foo=bar}", properties.toString());
	}

	@Test
	public void testGetBeanFromExistingContext() {
		assertEquals("foo", loader.getBean(configuration, String.class, true));
		assertEquals("foo", loader.getBean(configuration, String.class, false));
	}

	@Test
	public void testNamesGetBean() {
		assertEquals("foo", loader.getBean(configuration, String.class, true, "foo"));
	}

	@Test(expected = IllegalStateException.class)
	public void testReleaseContext() {
		assertEquals("foo", loader.getBean(configuration, String.class, true));
		loader.releaseContext(configuration);
		assertEquals("foo", loader.getBean(configuration, String.class, false));
	}

	@Test
	public void testGetJob() {
		Job job = loader.getJob(JobConfiguration.class, null, null);
		assertNotNull(job);
		assertEquals(JobConfiguration.class,
				job.getConfiguration().getClass(DefaultContextLoader.SPRING_CONFIG_LOCATION, Object.class));
	}

	@Test
	public void testGetBeanFromExistingContextCreatedForJob() {
		Job job = loader.getJob(JobConfiguration.class, null, null);
		assertNotNull(job);
		assertNotNull(loader.getBean(job.getConfiguration(), Job.class, false));
	}

	@Test
	public void testGetBeanFromExistingXmlContextCreatedForJob() {
		Job job = loader.getJob(ClassUtils.addResourcePathToPackagePath(getClass(), "job-context.xml"), null, null);
		assertNotNull(job);
		assertNotNull(loader.getBean(job.getConfiguration(), Job.class, false));
	}

	@Test(expected = IllegalStateException.class)
	public void testReleaseJob() {
		Job job = loader.getJob(JobConfiguration.class, null, null);
		assertNotNull(job);
		loader.releaseJob(job);
		assertNotNull(loader.getBean(job.getConfiguration(), Job.class, false));
	}

	@org.springframework.context.annotation.Configuration
	public static class TestConfiguration {
		@Bean
		public String foo() {
			return "foo";
		}

		@Bean
		TestContextHolder holder() {
			return new TestContextHolder();
		}
	}

	@org.springframework.context.annotation.Configuration
	public static class JobConfiguration {
		@Bean
		public Job foo() throws IOException {
			return new Job();
		}

		@Bean
		TestContextHolder holder() {
			return new TestContextHolder();
		}
	}

	public static class TestContextHolder implements ApplicationContextAware {
		private ApplicationContext applicationContext;

		public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
			this.applicationContext = applicationContext;
		}

		public ApplicationContext getApplicationContext() {
			return applicationContext;
		}
	}

}
