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
package org.springframework.yarn.boot;

import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.cluster.ManagedContainerClusterAppmaster;
import org.springframework.yarn.am.cluster.ContainerClusterStateMachineConfiguration;

/**
 * Tests for {@link YarnAppmasterAutoConfiguration}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnAppmasterAutoConfigurationTests {

	private AnnotationConfigApplicationContext context;

	@Rule
	public ExpectedException expected = ExpectedException.none();

	@After
	public void close() {
		if (context != null) {
			context.close();
		}
		System.clearProperty("HADOOP_TOKEN_FILE_LOCATION");
	}

	@Test
	public void testDefaultContext() throws Exception {
		System.setProperty("HADOOP_TOKEN_FILE_LOCATION", "xx/xx/xx/00001/container_tokens");
		context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils
				.addEnvironment(
						this.context,
						"spring.yarn.appmaster.appmasterClass:org.springframework.yarn.boot.YarnAppmasterAutoConfigurationTests$TestManagedContainerClusterAppmaster",
						"spring.yarn.appmaster.containercluster.enabled:true");
		context.register(ContainerClusterStateMachineConfiguration.class, TestConfigWithClass.class,
				YarnAppmasterAutoConfiguration.class, PropertyPlaceholderAutoConfiguration.class);
		context.refresh();
		assertNotNull(context.getBean(YarnAppmaster.class));
	}

	@Configuration
	public static class TestConfigWithClass {

	}

	public static class TestManagedContainerClusterAppmaster extends ManagedContainerClusterAppmaster {

		@Override
		protected void doStop() {
		}
	}

}
