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

public class SpringYarnAppmasterLocalizerPropertiesTests {

	@Test
	public void testAllPropertiesSet() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringYarnAppmasterLocalizerPropertiesTests" });
		SpringYarnAppmasterLocalizerProperties properties = context.getBean(SpringYarnAppmasterLocalizerProperties.class);
		assertThat(properties, notNullValue());

		List<String> pattern = properties.getPatterns();
		assertThat(pattern, notNullValue());
		assertThat(pattern.size(), is(2));
		assertThat(pattern.get(0), is("patterns1Foo"));
		assertThat(pattern.get(1), is("patterns2Foo"));

		List<String> names = properties.getPropertiesNames();
		assertThat(names, notNullValue());
		assertThat(names.size(), is(2));
		assertThat(names.get(0), is("name1Foo"));
		assertThat(names.get(1), is("name2Foo"));

		List<String> suffixes = properties.getPropertiesSuffixes();
		assertThat(suffixes, notNullValue());
		assertThat(suffixes.size(), is(2));
		assertThat(suffixes.get(0), is("suffix1Foo"));
		assertThat(suffixes.get(1), is("suffix2Foo"));

		assertThat(properties.getZipPattern(), is("zipPatternFoo"));

		context.close();
	}

	@Configuration
	@EnableConfigurationProperties({SpringYarnAppmasterLocalizerProperties.class})
	protected static class TestConfiguration {
	}

}
