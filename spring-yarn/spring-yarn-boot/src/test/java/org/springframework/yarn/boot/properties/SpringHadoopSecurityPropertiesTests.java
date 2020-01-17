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
import org.springframework.data.hadoop.security.SecurityAuthMethod;

/**
 * Tests for {@link SpringHadoopProperties} security bindings.
 *
 * @author Janne Valkealahti
 *
 */
public class SpringHadoopSecurityPropertiesTests {

	@Test
	public void testSecurityProperties() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringHadoopSecurityPropertiesTests" });
		SpringHadoopProperties properties = context.getBean(SpringHadoopProperties.class);
		assertThat(properties, notNullValue());
		assertThat(properties.getSecurity(), notNullValue());
		assertThat(properties.getSecurity().getAuthMethod(), is(SecurityAuthMethod.KERBEROS));
		assertThat(properties.getSecurity().getUserPrincipal(), is("userPrincipalFoo"));
		assertThat(properties.getSecurity().getUserKeytab(), is("userKeytabFoo"));
		assertThat(properties.getSecurity().getNamenodePrincipal(), is("hdfs/foo@LOCALDOMAIN"));
		assertThat(properties.getSecurity().getRmManagerPrincipal(), is("yarn/foo@LOCALDOMAIN"));
		context.close();
	}

	@Configuration
	@EnableConfigurationProperties({ SpringHadoopProperties.class, SpringYarnEnvProperties.class })
	protected static class TestConfiguration {
	}

}