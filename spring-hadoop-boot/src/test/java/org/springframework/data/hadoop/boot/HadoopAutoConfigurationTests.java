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
package org.springframework.data.hadoop.boot;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.hadoop.HadoopSystemConstants;

/**
 * Tests for {@link HadoopAutoConfiguration}.
 *
 * @author Janne Valkealahti
 *
 */
public class HadoopAutoConfigurationTests {

	private ConfigurableApplicationContext context;

	@Rule
	public ExpectedException expected = ExpectedException.none();

	@After
	public void close() {
		if (context != null) {
			context.close();
		}
	}

	@Test
	public void testConfigurationExists() throws Exception {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		this.context = ctx;
		ctx.register(HadoopAutoConfiguration.class);
		ctx.refresh();
		assertNotNull(context.getBean("hadoopConfiguration"));
	}

	@Test
	public void testConfigurationViaBootApp() {
		SpringApplication app = new SpringApplication(HadoopAutoConfiguration.class);
//		app.setWebEnvironment(false);
		ConfigurableApplicationContext ctx = app
				.run(new String[] { "--spring.config.name=HadoopAutoConfigurationTests1" });
		this.context = ctx;
		Configuration configuration = ctx.getBean(HadoopSystemConstants.DEFAULT_ID_CONFIGURATION, Configuration.class);
		assertThat(configuration, notNullValue());
		assertThat(configuration.get("fs.defaultFS"), is("hdfs://host:1234"));
		assertThat(configuration.get("mapreduce.jobhistory.address"), is("jobHistoryAddressFoo:10020"));
		assertThat(configuration.get("key1"), is("value1"));
		assertThat(configuration.get("key2"), is("value2"));
	}

}
