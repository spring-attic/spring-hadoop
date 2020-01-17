/*
 * Copyright 2014-2015 the original author or authors.
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
package org.springframework.data.hadoop.boot.properties;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

/**
 * Tests for {@link SpringHadoopProperties} bindings.
 *
 * @author Janne Valkealahti
 *
 */
public class SpringHadoopPropertiesTests {

	@Test
	public void testAllPropertiesSet1() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
//		app.setWebEnvironment(false);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringHadoopPropertiesTests1" });
		SpringHadoopProperties properties = context.getBean(SpringHadoopProperties.class);
		assertThat(properties, notNullValue());
		assertThat(properties.getFsUri(), is("fsUriFoo"));
		assertThat(properties.getResourceManagerAddress(), is("resourceManagerAddressFoo:321"));
		assertThat(properties.getResourceManagerSchedulerAddress(), is("resourceManagerSchedulerAddressFoo:123"));
		assertThat(properties.getResourceManagerHost(), is("resourceManagerAddressFoo"));
		assertThat(properties.getResourceManagerPort(), is(321));
		assertThat(properties.getResourceManagerSchedulerPort(), is(123));
		assertThat(properties.getResources(), notNullValue());
		assertThat(properties.getResources().size(), is(2));
		assertThat(properties.getResources().get(0), is("file:/fake-resource-1.xml"));
		assertThat(properties.getResources().get(1), is("classpath:/fake-resource-2.xml"));
		context.close();
	}

	@Test
	public void testAllPropertiesSet2() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
//		app.setWebEnvironment(false);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringHadoopPropertiesTests2" });
		SpringHadoopProperties properties = context.getBean(SpringHadoopProperties.class);
		assertThat(properties, notNullValue());
		assertThat(properties.getFsUri(), is("fsUriFoo"));
		assertThat(properties.getResourceManagerAddress(), is("resourceManagerHostFoo:321"));
		assertThat(properties.getResourceManagerSchedulerAddress(), is("resourceManagerHostFoo:123"));
		assertThat(properties.getResourceManagerHost(), is("resourceManagerHostFoo"));
		assertThat(properties.getResourceManagerPort(), is(321));
		assertThat(properties.getResourceManagerSchedulerPort(), is(123));
		assertThat(properties.getJobHistoryAddress(), is("jobHistoryAddressFoo:10020"));
		assertThat(properties.getResources(), nullValue());
		context.close();
	}

	@Test
	public void testHadoopConfigFromYaml() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
//		app.setWebEnvironment(false);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringHadoopPropertiesTests3" });
		SpringHadoopProperties properties = context.getBean(SpringHadoopProperties.class);
		assertThat(properties, notNullValue());
		assertThat(properties.getConfig(), notNullValue());
		assertThat(properties.getConfig().get("key1"), is("value1"));
		assertThat(properties.getConfig().get("key2"), is("value2"));
		assertThat(properties.getConfig().get("foo1.bar1"), is("jee1"));
		assertThat(properties.getConfig().get("foo2.bar21"), is("jee21"));
		assertThat(properties.getConfig().get("foo2.bar22"), is("jee22"));
		context.close();
	}

	@Test
	public void testHadoopConfigFromProperties() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
//		app.setWebEnvironment(false);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringHadoopPropertiesTests4" });
		SpringHadoopProperties properties = context.getBean(SpringHadoopProperties.class);
		assertThat(properties, notNullValue());
		assertThat(properties.getConfig(), notNullValue());
		assertThat(properties.getConfig().get("key1"), is("value1"));
		assertThat(properties.getConfig().get("key2"), is("value2"));
		assertThat(properties.getConfig().get("fs.defaultFS"), is("defaultFSValue"));
		context.close();
	}

	@Configuration
	@EnableConfigurationProperties({ SpringHadoopProperties.class })
	protected static class TestConfiguration {
	}

}