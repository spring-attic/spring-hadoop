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
package org.springframework.data.hadoop.hbase;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;

import static org.junit.Assert.assertEquals;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class HBaseCfgTest {

	@Resource
	Configuration hbaseConfiguration;
	@Autowired
	Job job;

	@Test
	public void testConfigProperties() throws Exception {
		Assert.notNull(hbaseConfiguration);
		assertEquals("bucket", hbaseConfiguration.get("head"));
		assertEquals("main", hbaseConfiguration.get("cfg"));
	}

	@Test
	public void testLocalConfigOverride() throws Exception {
		assertEquals("anothervalue", hbaseConfiguration.get("someparam"));
	}


	@Test
	public void testJobCfg() throws Exception {
		job.waitForCompletion(true);
	}
}
