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

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Jarred Li
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class HadoopJobDetailBeanTest {
	
	@Autowired
	private HadoopJobDetailBean jobDetailBean;
	
	@Autowired
	private ApplicationContext applicationContext;

	/**
	 * Test method for {@link org.springframework.data.hadoop.mapreduce.HadoopJobDetailBean#afterPropertiesSet()}.
	 */
	@Test
	public void testAfterPropertiesSet() {
		jobDetailBean.setApplicationContext(applicationContext);
		assertNotNull(jobDetailBean.getApplicationContext());
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.mapreduce.HadoopJobDetailBean#setJobNames(java.util.Set)}.
	 */
	@Test
	public void testSetJobNames() {
		Set<String> names = new HashSet<String>();
		names.add("job1");
		jobDetailBean.setJobNames(names);
		assertEquals(1,jobDetailBean.getJobNames().size());
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.mapreduce.HadoopJobDetailBean#setWaitForJobs(boolean)}.
	 */
	@Test
	public void testSetWaitForJobs() {
		jobDetailBean.setWaitForJobs(true);
		assertTrue(jobDetailBean.isWaitForJobs());
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.mapreduce.HadoopJobDetailBean#setApplicationContext(org.springframework.context.ApplicationContext)}.
	 */
	@Test
	public void testSetApplicationContextApplicationContext() {
		jobDetailBean.setApplicationContext(applicationContext);
		assertNotNull(jobDetailBean.getApplicationContext());
	}

}
