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
package org.springframework.data.hadoop.hbase;



import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Jarred Li
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/org/springframework/data/hadoop/hbase/factory.xml")
public class HbaseJobFactoryBeanTest {


	
	@Autowired
	private HbaseJobFactoryBean factoryBean;
	
	/**
	 * Test method for {@link org.springframework.data.hadoop.hbase.HbaseJobFactoryBean#processJob(org.apache.hadoop.mapreduce.Job)}.
	 */
	@Test
	public void testProcessJob() {
		try {
			Job job = factoryBean.getObject();
			Configuration config = job.getConfiguration();
			String scan = config.get(TableInputFormat.SCAN);
			Assert.assertNotNull(scan, "scan is empty");
		} catch (Exception e) {
			fail("exception was throw in testing processJob.");
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.hbase.HbaseJobFactoryBean#getInputTable()}.
	 */
	@Test
	public void testGetInputTable() {
		String inputTable = factoryBean.getInputTable();
		Assert.assertEquals("likes",inputTable);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.hbase.HbaseJobFactoryBean#getOutputTable()}.
	 */
	@Test
	public void testGetOutputTable() {
		String outputTable = factoryBean.getOutputTable();
		Assert.assertEquals("numOfLikes",outputTable);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.hbase.HbaseJobFactoryBean#isCacheBlocks()}.
	 */
	@Test
	public void testIsCacheBlocks() {
		boolean isCacheBlock = factoryBean.isCacheBlocks();
		Assert.assertTrue(isCacheBlock);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.hbase.HbaseJobFactoryBean#getCachingNum()}.
	 */
	@Test
	public void testGetCachingNum() {
		int num = factoryBean.getCachingNum();
		Assert.assertEquals(600, num);
	}

}
