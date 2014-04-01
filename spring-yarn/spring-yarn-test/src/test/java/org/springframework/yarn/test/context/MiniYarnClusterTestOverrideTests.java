/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.yarn.test.context;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Tests using {@link MiniYarnClusterTest}.
 *
 * @author Janne Valkealahti
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@MiniYarnClusterTest(
		classes = MiniYarnClusterTestOverrideTests.Config.class,
		configName = "yarnCustomConfiguration",
		clusterName = "yarnCustomCluster",
		id = "custom")
public class MiniYarnClusterTestOverrideTests {

	@Autowired
	private ApplicationContext ctx;

	@Test
	public void testLoaderAndConfig() {
		assertNotNull(ctx);
		assertTrue(ctx.containsBean("yarnCustomCluster"));
		assertTrue(ctx.containsBean("yarnCustomConfiguration"));
		Configuration config = (Configuration) ctx.getBean("yarnCustomConfiguration");
		assertNotNull(config);
	}

	@org.springframework.context.annotation.Configuration
	public static class Config {
		// we're testing custom name so need to change config
		// so that spring-test doesn't cache our context and
		// we don't want to dirty context for all tests.
	}

}
