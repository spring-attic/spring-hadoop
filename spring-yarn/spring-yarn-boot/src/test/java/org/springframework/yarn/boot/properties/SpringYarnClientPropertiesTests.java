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

import java.util.List;

import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

/**
 * Tests for {@link SpringYarnClientProperties} bindings.
 *
 * @author Janne Valkealahti
 *
 */
public class SpringYarnClientPropertiesTests {

	@Test
	public void testAllPropertiesSet() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringYarnClientPropertiesTests" });
		SpringYarnClientProperties properties = context.getBean(SpringYarnClientProperties.class);
		assertThat(properties, notNullValue());

		List<String> files = properties.getFiles();
		assertThat(files, notNullValue());
		assertThat(files.size(), is(2));
		assertThat(files.get(0), is("files1Foo"));
		assertThat(files.get(1), is("files2Foo"));

		assertThat(properties.getPriority(), is(234));
		assertThat(properties.getQueue(), is("queueFoo"));
		assertThat(properties.getLabelExpression(), is("labelExpressionFoo"));

		assertThat(properties.getClientClass(), is("clientClassFoo"));

		assertThat(properties.getStartup().getAction(), is("submit"));

		context.close();
	}

	@Configuration
	@EnableConfigurationProperties({ SpringYarnClientProperties.class})
	protected static class TestConfiguration {
	}

}