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
package org.springframework.yarn.boot.properties;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.yarn.batch.support.YarnBatchProperties.JobProperties;

/**
 * Tests for {@link SpringYarnBatchProperties} bindings.
 *
 * @author Janne Valkealahti
 *
 */
public class SpringYarnBatchPropertiesTests {

	@Test
	public void testAllPropertiesSetYml() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringYarnBatchPropertiesTests" });
		SpringYarnBatchProperties properties = context.getBean(SpringYarnBatchProperties.class);
		assertThat(properties, notNullValue());
		assertThat(properties.isEnabled(), is(true));
		assertThat(properties.getName(), is("nameFoo1"));

		assertThat(properties.getJobs(), notNullValue());
		assertThat(properties.getJobs().size(), is(1));
		assertThat(properties.getJobProperties("jobsName1"), notNullValue());

		JobProperties jobProperties = properties.getJobProperties("jobsName1");
		assertThat(jobProperties.isEnabled(), is(true));
		assertThat(jobProperties.isFailNext(), is(true));
		assertThat(jobProperties.isFailRestart(), is(true));
		assertThat(jobProperties.isNext(), is(true));
		assertThat(jobProperties.isRestart(), is(true));
		assertThat(jobProperties.getParameters(), notNullValue());
		assertThat(jobProperties.getParameters().size(), is(2));
		assertThat((String)jobProperties.getParameters().get("job1key1"), is("job1val1"));
		assertThat((String)jobProperties.getParameters().get("job1key2"), is("job1val2"));
		context.close();
	}

	@Test
	public void testAllPropertiesSetProperties() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringYarnBatchPropertiesTests2" });
		SpringYarnBatchProperties properties = context.getBean(SpringYarnBatchProperties.class);
		assertThat(properties, notNullValue());
		assertThat(properties.isEnabled(), is(true));
		assertThat(properties.getName(), is("nameFoo1"));

		assertThat(properties.getJobs(), notNullValue());
		assertThat(properties.getJobs().size(), is(1));
		assertThat(properties.getJobProperties("jobsName1"), notNullValue());

		JobProperties jobProperties = properties.getJobProperties("jobsName1");
		assertThat(jobProperties.isEnabled(), is(true));
		assertThat(jobProperties.isFailNext(), is(true));
		assertThat(jobProperties.isFailRestart(), is(true));
		assertThat(jobProperties.isNext(), is(true));
		assertThat(jobProperties.isRestart(), is(true));
		assertThat(jobProperties.getParameters(), notNullValue());
		assertThat(jobProperties.getParameters().size(), is(2));
		assertThat((String)jobProperties.getParameters().get("job1key1"), is("job1val1"));
		assertThat((String)jobProperties.getParameters().get("job1key2"), is("job1val2"));
		context.close();
	}
	
	@Configuration
	@EnableConfigurationProperties({SpringYarnBatchProperties.class})
	protected static class TestConfiguration {
	}

}