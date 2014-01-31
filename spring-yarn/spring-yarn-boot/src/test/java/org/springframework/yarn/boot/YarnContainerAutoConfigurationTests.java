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
package org.springframework.yarn.boot;

import static org.junit.Assert.assertNotNull;

import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.boot.support.ContainerLauncherRunner;
import org.springframework.yarn.container.YarnContainer;

/**
 * Tests for {@link YarnContainerAutoConfiguration}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnContainerAutoConfigurationTests {

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
		System.setProperty("HADOOP_TOKEN_FILE_LOCATION", "xx/xx/xx/00002");
		context = new AnnotationConfigApplicationContext();
		context.register(TestConfigWithClass.class, YarnContainerAutoConfiguration.class);
		context.refresh();
		assertNotNull(context.getBean(YarnContainer.class));
		assertNotNull(context.getBean(ContainerLauncherRunner.class));
	}

	@Configuration
	public static class TestConfigWithClass {

		@Bean(name=YarnSystemConstants.DEFAULT_ID_CONTAINER_CLASS)
		public Class<? extends YarnContainer> yarnContainerClass() {
			return TestContainer.class;
		}

	}

	public static class TestContainer implements YarnContainer {

		@Override
		public void run() {
		}

		@Override
		public void setEnvironment(Map<String, String> environment) {
		}

		@Override
		public void setParameters(Properties parameters) {
		}

	}

}
