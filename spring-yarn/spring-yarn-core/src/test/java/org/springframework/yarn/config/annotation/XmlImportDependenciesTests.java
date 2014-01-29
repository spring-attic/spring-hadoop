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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.builders.YarnClientConfigurer;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=AnnotationConfigContextLoader.class)
public class XmlImportDependenciesTests {

	@Autowired
	private ApplicationContext ctx;

	@Test
	public void testSimpleConfig() throws Exception {
		assertNotNull(ctx);
		assertTrue(ctx.containsBean(YarnSystemConstants.DEFAULT_ID_CLIENT));
		YarnClient client = ctx.getBean(YarnSystemConstants.DEFAULT_ID_CLIENT, YarnClient.class);
		assertNotNull(client);

		ctx.containsBean("dependencyBean");
		DependencyBean dependencyBean = ctx.getBean(DependencyBean.class);
		assertNotNull(dependencyBean.getConfiguration());
		assertNotNull(dependencyBean.getClient());
	}

	@Configuration
	@EnableYarn(enable=Enable.CLIENT)
	@ImportResource("org/springframework/yarn/config/annotation/XmlImportDependencies.xml")
	static class Config extends SpringYarnConfigurerAdapter {

		@Override
		public void configure(YarnClientConfigurer client) throws Exception {
			client
				.withMasterRunner()
				.and();
		}

	}

}
