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

package org.springframework.hadoop.examples;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.hadoop.test.HadoopSetUp;

/**
 * @author Dave Syer
 * 
 */
public class SortTests {

	@Rule
	public HadoopSetUp setUp = HadoopSetUp.localOnly();

	@Before
	public void init() throws Exception {
		setUp.delete("target/output");
	}

	@Test
	public void testNumReduceTasks() throws Exception {
		Sort sort = new Sort();
		sort.setConf(new Configuration());
		assertEquals(0, sort.getNumReduceTasks());
	}

	@Test
	public void testTaskTrackers() throws Exception {
		Sort sort = new Sort();
		sort.setConf(new Configuration());
		@SuppressWarnings("deprecation")
		JobClient client = new JobClient(new org.apache.hadoop.mapred.JobConf(sort.getConf()));
		ClusterStatus cluster = client.getClusterStatus();
		assertEquals(1, cluster.getTaskTrackers());
	}

	@Test
	public void testRandomWriter() throws Exception {
		setUp.delete("target/binary");
		// Always local because the configuration hasn't been set up for the cluster
		new RandomWriter().run(new String[0]);
	}

	@Test
	public void testSort() throws Exception {
		setUp.copy("target/binary", "target/input");
		// Always local because the configuration hasn't been set up for the cluster
		new Sort().run(new String[0]);
	}

}
