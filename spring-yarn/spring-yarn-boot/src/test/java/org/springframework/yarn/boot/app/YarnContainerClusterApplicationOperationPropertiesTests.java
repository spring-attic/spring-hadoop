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
package org.springframework.yarn.boot.app;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.yarn.boot.app.YarnContainerClusterApplication.OperationProperties;

public class YarnContainerClusterApplicationOperationPropertiesTests {

	@Test
	public void testProjectionDataProperties() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app.run(new String[] {
				"--spring.yarn.internal.container-cluster-application.projectionData.any=1",
				"--spring.yarn.internal.container-cluster-application.projectionData.hosts.host1=1",
				"--spring.yarn.internal.container-cluster-application.projectionData.racks.rack1=1",
				"--spring.yarn.internal.container-cluster-application.projectionData.hosts.host2=2",
				"--spring.yarn.internal.container-cluster-application.projectionData.racks.rack2=2" });
		OperationProperties properties = context.getBean(OperationProperties.class);
		assertThat(properties, notNullValue());
		assertThat(properties.getProjectionData().getAny(), is(1));
		assertThat(properties.getProjectionData().getHosts().get("host1"), is(1));
		assertThat(properties.getProjectionData().getHosts().get("host2"), is(2));
		assertThat(properties.getProjectionData().getRacks().get("rack1"), is(1));
		assertThat(properties.getProjectionData().getRacks().get("rack2"), is(2));
		context.close();
	}

	@Configuration
	@EnableConfigurationProperties({ OperationProperties.class })
	protected static class TestConfiguration {
	}

}
