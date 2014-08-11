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
package org.springframework.yarn.config.annotation;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.yarn.TestUtils;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.allocate.ContainerAllocator;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterConfigurer;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=AnnotationConfigContextLoader.class)
public class SpringYarnConfigurationMasterAllocatorTests {

	@Autowired
	private ApplicationContext ctx;

	@Test
	public void testSimpleConfig() throws Exception {
		YarnAppmaster master = (YarnAppmaster) ctx.getBean("yarnAppmaster");
		assertNotNull(master);

		ContainerAllocator allocator = TestUtils.callMethod("getAllocator", master);
		assertNotNull(allocator);
	}

	@Configuration
	@EnableYarn(enable=Enable.APPMASTER)
	static class Config extends SpringYarnConfigurerAdapter {

		// TODO: need to fake appmaster for it to not try to connect hadoop
		@Override
		public void configure(YarnAppmasterConfigurer master) throws Exception {
			master
				.withContainerAllocator()
					.priority(1)
					.memory(64)
					.virtualCores(1)
					.locality(true)
					.withCollection("cluster1")
						.priority(2)
						.and()
					.withCollection("cluster2")
						.priority(3)
						.memory(64)
						.virtualCores(1);
		}

	}

}
