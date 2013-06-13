/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.yarn.test;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestCluster {

	private static final Log LOG = LogFactory.getLog(TestCluster.class);

	protected static MiniYARNCluster yarnCluster = null;
	protected static Configuration conf = new YarnConfiguration();

	@BeforeClass
	public static void setup() throws InterruptedException, IOException, URISyntaxException {

		LOG.info("Starting up YARN cluster");
		conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
		conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
		if (yarnCluster == null) {
			yarnCluster = new MiniYARNCluster(TestCluster.class.getSimpleName(), 1, 1, 1);
			yarnCluster.init(conf);
			yarnCluster.start();
		}
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
		}
	}

	@AfterClass
	public static void tearDown() throws IOException {
		if (yarnCluster != null) {
			yarnCluster.stop();
			yarnCluster = null;
		}
	}

//	@Test(timeout = 30000)
//	public void testDummy() throws Exception {
//	}

}
