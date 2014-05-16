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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Thomas Risberg
 */
public class ConfigurationUtilsTest {

	@Test
	public void testConfigurationCreation() throws Exception {
		Properties prop = new Properties();
		prop.setProperty("name", "test1");
		Configuration cfg = ConfigurationUtils.createFrom(null, prop);
		assertEquals("test1", cfg.get("name"));
		Configuration cfg2 = ConfigurationUtils.createFrom(cfg, prop);
		assertEquals("test1", cfg2.get("name"));
		assertTrue(cfg2 instanceof Configuration);
		assertTrue(!(cfg2 instanceof JobConf));
	}

	@Test
	public void testJobConfCreation() throws Exception {
		Properties prop = new Properties();
		prop.setProperty("name", "test2");
		Configuration cfg = JobConfUtils.createFrom(null, prop);
		assertEquals("test2", cfg.get("name"));
		Configuration cfg2 = ConfigurationUtils.createFrom(cfg, prop);
		assertEquals("test2", cfg2.get("name"));
		assertTrue(cfg2 instanceof JobConf);
	}

	@Test
	public void testConfigurationProperties() throws Exception {
		Properties prop = new Properties();
		prop.setProperty("mapred.reduce.tasks", "8");
		prop.setProperty("mapred.map.tasks", "4");

		Configuration cfg = ConfigurationUtils.createFrom(null, prop);

		assertEquals("8", cfg.get("mapred.reduce.tasks"));
		assertEquals("4", cfg.get("mapred.map.tasks"));
		@SuppressWarnings("deprecation")
		Job j = new Job(cfg);
		assertEquals("8", j.getConfiguration().get("mapred.reduce.tasks"));
		assertEquals("4", j.getConfiguration().get("mapred.map.tasks"));
	}
}
