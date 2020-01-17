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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.cluster.ContainerClusterStateMachineConfiguration;
import org.springframework.yarn.am.cluster.ManagedContainerClusterAppmaster;
import org.springframework.yarn.am.grid.GridProjection;
import org.springframework.yarn.am.grid.GridProjectionFactory;
import org.springframework.yarn.am.grid.GridProjectionFactoryLocator;
import org.springframework.yarn.am.grid.support.ProjectionData;

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
		TestPropertyValues.of("spring.yarn.appmaster.appmasterClass=org.springframework.yarn.boot.YarnAppmasterAutoConfigurationTests$TestManagedContainerClusterAppmaster")
		.and("spring.yarn.appmaster.containercluster.enabled=true")
		.applyTo(context);
		
		context.register(ContainerClusterStateMachineConfiguration.class,
				YarnAppmasterAutoConfiguration.class, PropertyPlaceholderAutoConfiguration.class);
		context.refresh();
		assertNotNull(context.getBean(YarnAppmaster.class));
		YarnAppmaster appmaster = context.getBean(YarnAppmaster.class);
		GridProjectionFactoryLocator locator = TestUtils.readField("gridProjectionFactoryLocator", appmaster);
		Set<GridProjectionFactory> factories = TestUtils.readField("factories", locator);
		assertThat(factories.size(), is(1));
		GridProjectionFactory factory = factories.iterator().next();
		assertThat(factory.getRegisteredProjectionTypes().size(), is(1));
		assertThat(factory.getRegisteredProjectionTypes(), containsInAnyOrder("default"));
	}

	@Test
	public void testOverrideDefaultGridProjectionFactory() throws Exception {
		System.setProperty("HADOOP_TOKEN_FILE_LOCATION", "xx/xx/xx/00001/container_tokens");
		context = new AnnotationConfigApplicationContext();
		
		TestPropertyValues.of("spring.yarn.appmaster.appmasterClass=org.springframework.yarn.boot.YarnAppmasterAutoConfigurationTests$TestManagedContainerClusterAppmaster")
        .and("spring.yarn.appmaster.containercluster.enabled=true")
        .applyTo(context);

		context.register(ContainerClusterStateMachineConfiguration.class, TestConfig1WithClass.class,
				YarnAppmasterAutoConfiguration.class, PropertyPlaceholderAutoConfiguration.class);
		context.refresh();
		assertNotNull(context.getBean(YarnAppmaster.class));
		YarnAppmaster appmaster = context.getBean(YarnAppmaster.class);
		GridProjectionFactoryLocator locator = TestUtils.readField("gridProjectionFactoryLocator", appmaster);
		Set<GridProjectionFactory> factories = TestUtils.readField("factories", locator);
		assertThat(factories.size(), is(1));
		GridProjectionFactory factory = factories.iterator().next();
		assertThat(factory.getRegisteredProjectionTypes().size(), is(1));
		assertThat(factory.getRegisteredProjectionTypes(), containsInAnyOrder("foo"));
	}

	@Test
	public void testAddCustomGridProjectionFactory() throws Exception {
		System.setProperty("HADOOP_TOKEN_FILE_LOCATION", "xx/xx/xx/00001/container_tokens");
		context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("spring.yarn.appmaster.appmasterClass=org.springframework.yarn.boot.YarnAppmasterAutoConfigurationTests$TestManagedContainerClusterAppmaster")
          .and("spring.yarn.appmaster.containercluster.enabled=true")
          .applyTo(context);

		context.register(ContainerClusterStateMachineConfiguration.class, TestConfig2WithClass.class,
				YarnAppmasterAutoConfiguration.class, PropertyPlaceholderAutoConfiguration.class);
		context.refresh();
		assertNotNull(context.getBean(YarnAppmaster.class));
		YarnAppmaster appmaster = context.getBean(YarnAppmaster.class);
		GridProjectionFactoryLocator locator = TestUtils.readField("gridProjectionFactoryLocator", appmaster);
		Set<GridProjectionFactory> factories = TestUtils.readField("factories", locator);
		assertThat(factories.size(), is(2));
	}

	@Configuration
	public static class TestConfig1WithClass {

		@Bean
		public GridProjectionFactory defaultGridProjectionFactory() {
			return new TestGridProjectionFactory();
		}

	}

	@Configuration
	public static class TestConfig2WithClass {

		@Bean
		public GridProjectionFactory testGridProjectionFactory() {
			return new TestGridProjectionFactory();
		}

	}

	public static class TestGridProjectionFactory implements GridProjectionFactory {

		@Override
		public GridProjection getGridProjection(ProjectionData projectionData, org.apache.hadoop.conf.Configuration configuration) {
			return null;
		}

		@Override
		public Set<String> getRegisteredProjectionTypes() {
			return new HashSet<String>(Arrays.asList("foo"));
		}

	}

	public static class TestManagedContainerClusterAppmaster extends ManagedContainerClusterAppmaster {

		@Override
		protected void doStop() {
		}
	}

}
