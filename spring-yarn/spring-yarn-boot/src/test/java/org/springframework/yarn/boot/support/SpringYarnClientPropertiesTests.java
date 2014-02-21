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
package org.springframework.yarn.boot.support;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.springframework.boot.SpringApplication;
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
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringYarnClientPropertiesTests" });
		SpringYarnClientProperties properties = context.getBean(SpringYarnClientProperties.class);
		assertThat(properties, notNullValue());
		assertThat(properties.getAppmasterFile(), is("appmasterFileFoo"));

		Map<String, String> arguments = properties.getArguments();
		assertThat(arguments, notNullValue());
		assertThat(arguments.size(), is(2));
		assertThat(arguments.get("argumentsKeyFoo1"), is("argumentsValFoo1"));
		assertThat(arguments.get("argumentsKeyFoo2"), is("argumentsValFoo2"));

		List<String> classpath = properties.getClasspath();
		assertThat(classpath, notNullValue());
		assertThat(classpath.size(), is(2));
		assertThat(classpath.get(0), is("classpath1Foo"));
		assertThat(classpath.get(1), is("classpath2Foo"));

		List<String> files = properties.getFiles();
		assertThat(files, notNullValue());
		assertThat(files.size(), is(2));
		assertThat(files.get(0), is("files1Foo"));
		assertThat(files.get(1), is("files2Foo"));

		assertThat(properties.getMasterRunner(), is("masterRunnerFoo"));
		assertThat(properties.getMemory(), is("memoryFoo"));

		List<String> options = properties.getOptions();
		assertThat(options, notNullValue());
		assertThat(options.size(), is(2));
		assertThat(options.get(0), is("options1Foo"));
		assertThat(options.get(1), is("options2Foo"));

		assertThat(properties.getPriority(), is(234));
		assertThat(properties.getQueue(), is("queueFoo"));
		assertThat(properties.getRawFileContents(), nullValue());
		assertThat(properties.getVirtualCores(), is(123));

		assertThat(properties.isDefaultYarnAppClasspath(), is(false));
		assertThat(properties.isIncludeBaseDirectory(), is(false));
		assertThat(properties.isIncludeSystemEnv(), is(false));
		assertThat(properties.getDelimiter(), is(":"));

		List<String> pattern = properties.getLocalizerPatterns();
		assertThat(pattern, notNullValue());
		assertThat(pattern.size(), is(2));
		assertThat(pattern.get(0), is("patterns1Foo"));
		assertThat(pattern.get(1), is("patterns2Foo"));

		List<String> names = properties.getLocalizerPropertiesNames();
		assertThat(names, notNullValue());
		assertThat(names.size(), is(2));
		assertThat(names.get(0), is("name1Foo"));
		assertThat(names.get(1), is("name2Foo"));

		List<String> suffixes = properties.getLocalizerPropertiesSuffixes();
		assertThat(suffixes, notNullValue());
		assertThat(suffixes.size(), is(2));
		assertThat(suffixes.get(0), is("suffix1Foo"));
		assertThat(suffixes.get(1), is("suffix2Foo"));

		assertThat(properties.getLocalizerZipPattern(), is("patternFoo"));

		context.close();
	}

	@Configuration
	@EnableConfigurationProperties({ SpringYarnClientProperties.class})
	protected static class TestConfiguration {
	}

}