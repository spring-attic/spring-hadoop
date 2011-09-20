/*
 * Copyright 2006-2011 the original author or authors.
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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.junit.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.data.hadoop.context.DefaultContextLoader;
import org.springframework.data.hadoop.context.HadoopApplicationContextUtils;
import org.springframework.data.hadoop.mapreduce.AutowiringPartitioner;
import org.springframework.stereotype.Component;


/**
 * @author Dave Syer
 *
 */
public class AutowiringPartitionerTests {
	
	@Test
	public void testPartitioner() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setClass(DefaultContextLoader.SPRING_CONFIG_LOCATION, TestConfiguration.class, Object.class);
		HadoopApplicationContextUtils.getBean(configuration, String.class);
		AutowiringPartitioner<Integer, String> partitioner = new AutowiringPartitioner<Integer, String>();
		partitioner.setConf(configuration);
		assertEquals(new Integer(123).hashCode(), partitioner.getPartition(123, "bar", 10));
	}
	
	@Component
	public static class TestConfiguration {
		@Bean
		public String getDummy() {
			return "foo";
		}
		@Bean
		public Partitioner<Integer, String> getPartitioner() {
			return new Partitioner<Integer, String>() {
				@Override
				public int getPartition(Integer key, String value, int numPartitions) {
					return key.hashCode();
				}
			};
		}
	}

}
