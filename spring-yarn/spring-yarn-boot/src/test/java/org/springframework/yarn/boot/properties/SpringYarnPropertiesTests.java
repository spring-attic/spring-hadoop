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
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

/**
 * Tests for {@link SpringYarnProperties} bindings.
 *
 * @author Janne Valkealahti
 *
 */
public class SpringYarnPropertiesTests {

	@Test
	public void testAllPropertiesSet() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
		app.setWebEnvironment(false);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringYarnPropertiesTests" });
		SpringYarnProperties properties = context.getBean(SpringYarnProperties.class);
		assertThat(properties, notNullValue());
		assertThat(properties.getApplicationDir(), is("applicationDirFoo"));
		assertThat(properties.getApplicationBaseDir(), is("applicationBaseDirFoo"));
		assertThat(properties.getApplicationVersion(), is("applicationVersionFoo"));
		assertThat(properties.getAppName(), is("appNameFoo"));
		assertThat(properties.getAppType(), is("appTypeFoo"));
		assertThat(properties.getStagingDir(), is("stagingDirFoo"));
		assertThat(properties.getDefaultYarnAppClasspath(), is("defaultYarnAppClasspathFoo"));
		context.close();
	}

	@Configuration
	@EnableConfigurationProperties({ SpringYarnProperties.class, SpringYarnEnvProperties.class })
	protected static class TestConfiguration {
	}

}