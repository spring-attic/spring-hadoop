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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

public class SpringYarnAppmasterLaunchContextPropertiesTests {

	@Test
	public void testAllPropertiesSet() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringYarnAppmasterLaunchContextPropertiesTests" });
		SpringYarnAppmasterLaunchContextProperties properties = context.getBean(SpringYarnAppmasterLaunchContextProperties.class);
		assertThat(properties, notNullValue());
		assertThat(properties.getArchiveFile(), is("archiveFileFoo"));

		Map<String, String> arguments = properties.getArguments();
		assertThat(arguments, notNullValue());
		assertThat(arguments.size(), is(2));
		assertThat(arguments.get("argumentsKeyFoo1"), is("argumentsValFoo1"));
		assertThat(arguments.get("argumentsKeyFoo2"), is("argumentsValFoo2"));

		List<String> argumentsList = properties.getArgumentsList();
		assertThat(argumentsList, notNullValue());
		assertThat(argumentsList.size(), is(2));
		assertThat(argumentsList, contains("argumentsListFoo1", "argumentsListFoo2"));

		List<String> classpath = properties.getContainerAppClasspath();
		assertThat(classpath, notNullValue());
		assertThat(classpath.size(), is(2));
		assertThat(classpath.get(0), is("classpath1Foo"));
		assertThat(classpath.get(1), is("classpath2Foo"));

		assertThat(properties.getRunnerClass(), is("runnerClassFoo"));

		List<String> options = properties.getOptions();
		assertThat(options, notNullValue());
		assertThat(options.size(), is(2));
		assertThat(options.get(0), is("options1Foo"));
		assertThat(options.get(1), is("options2Foo"));

		assertThat(properties.isLocality(), is(true));
		assertThat(properties.isUseYarnAppClasspath(), is(true));
		assertThat(properties.isUseMapreduceAppClasspath(), is(true));
		assertThat(properties.isIncludeBaseDirectory(), is(false));
		assertThat(properties.isIncludeLocalSystemEnv(), is(true));
		assertThat(properties.getPathSeparator(), is(":"));

		context.close();
	}

	@Configuration
	@EnableConfigurationProperties({SpringYarnAppmasterLaunchContextProperties.class})
	protected static class TestConfiguration {
	}

}
