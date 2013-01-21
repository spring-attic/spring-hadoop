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
package org.springframework.data.hadoop.configuration;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * See SHDP-92: https://jira.springsource.org/browse/SHDP-92
 * @author Costin Leau
 */
public class UrlInfiniteLoopTest {

	@Test
	public void testInfiniteLoop() throws Exception {
		ConfigurationFactoryBean factory = new ConfigurationFactoryBean();
		factory.setRegisterUrlHandler(true);
		factory.afterPropertiesSet();
		assertNotNull(factory.getObject());
		FileSystem fs = FileSystem.get(factory.getObject());
		assertNotNull(fs);
	}

	@Test
	public void testConfigurationProperties() throws Exception {
		Properties prop = new Properties();
		prop.setProperty("mapred.reduce.tasks", "8");
		prop.setProperty("mapred.map.tasks", "4");

		Configuration cfg = ConfigurationUtils.createFrom(null, prop);

		assertEquals("8", cfg.get("mapred.reduce.tasks"));
		assertEquals("4", cfg.get("mapred.map.tasks"));
		Job j = new Job(cfg);
		assertEquals("8", j.getConfiguration().get("mapred.reduce.tasks"));
		assertEquals("4", j.getConfiguration().get("mapred.map.tasks"));
	}
}
