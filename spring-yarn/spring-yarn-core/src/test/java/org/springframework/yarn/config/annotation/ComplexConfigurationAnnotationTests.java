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
package org.springframework.yarn.config.annotation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import javax.annotation.Resource;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.builders.YarnConfigConfigurer;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=AnnotationConfigContextLoader.class)
public class ComplexConfigurationAnnotationTests {

	@Autowired
	private ApplicationContext ctx;

	@Resource(name = YarnSystemConstants.DEFAULT_ID_CONFIGURATION)
	private YarnConfiguration yarnConfiguration;

	@Test
	public void testComplexConfig() throws Exception {
		assertNotNull(yarnConfiguration);
		assertEquals("jee", yarnConfiguration.get("test.foo"));
		assertEquals("10.10.10.10:8032", yarnConfiguration.get("yarn.resourcemanager.address"));
		assertEquals("test-site-1.xml", yarnConfiguration.get("resource.property"));
		assertEquals("test-site-2.xml", yarnConfiguration.get("resource.property.2"));
	}

	@Configuration
	@EnableYarn(enable=Enable.BASE)
	static class Config extends SpringYarnConfigurerAdapter {

		@Override
		public void configure(YarnConfigConfigurer config) throws Exception {
			config
				.fileSystemUri("hdfs://foo.uri")
				.resourceManagerAddress("10.10.10.10:8032")
				.withResources()
					.resource("classpath:/test-site-1.xml")
					.resource("classpath:/test-site-2.xml")
					.and()
				.withProperties()
					.property("test.foo.2", "jee2")
					.property("test.foo", "jee");
		}

	}

}
