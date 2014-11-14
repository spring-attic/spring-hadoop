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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.boot.actuate.autoconfigure.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.EndpointMBeanExportAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.web.EmbeddedServletContainerAutoConfiguration;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import org.springframework.yarn.boot.SpringApplicationCallback;
import org.springframework.yarn.boot.SpringApplicationTemplate;
import org.springframework.yarn.boot.support.SpringYarnBootUtils;
import org.springframework.yarn.client.YarnClient;

/**
 * Generic Spring Boot client application used to kill Spring Yarn Boot based apps.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@EnableAutoConfiguration(exclude = { EmbeddedServletContainerAutoConfiguration.class, WebMvcAutoConfiguration.class,
		JmxAutoConfiguration.class, BatchAutoConfiguration.class, JmxAutoConfiguration.class,
		EndpointMBeanExportAutoConfiguration.class, EndpointAutoConfiguration.class })
public class YarnKillApplication extends AbstractClientApplication<String, YarnKillApplication> {

	public String run() {
		return run(new String[0]);
	}

	public String run(String... args) {
		SpringApplicationBuilder builder = new SpringApplicationBuilder();
		builder.web(false);
		builder.sources(YarnKillApplication.class, OperationProperties.class);
		SpringYarnBootUtils.addSources(builder, sources.toArray(new Object[0]));
		SpringYarnBootUtils.addProfiles(builder, profiles.toArray(new String[0]));
		if (StringUtils.hasText(applicationBaseDir)) {
			appProperties.setProperty("spring.yarn.applicationDir", applicationBaseDir + applicationVersion + "/");
		}
		SpringYarnBootUtils.addApplicationListener(builder, appProperties);
		SpringApplicationTemplate template = new SpringApplicationTemplate(builder);

		return template.execute(new SpringApplicationCallback<String>() {

			@Override
			public String runWithSpringApplication(ApplicationContext context) throws Exception {
				YarnClient client = context.getBean(YarnClient.class);
				OperationProperties operationProperties = context.getBean(OperationProperties.class);
				ApplicationId applicationId = ConverterUtils.toApplicationId(operationProperties.getApplicationId());
				ApplicationReport report = client.getApplicationReport(applicationId);
				if (report.getYarnApplicationState() == YarnApplicationState.FINISHED
						|| report.getYarnApplicationState() == YarnApplicationState.KILLED
						|| report.getYarnApplicationState() == YarnApplicationState.FAILED) {
					return "Application " + applicationId + " is not running";
				} else {
					client.killApplication(applicationId);
					return "Kill request for " + applicationId + " sent";
				}
			}

		}, args);

	}

	@Override
	protected YarnKillApplication getThis() {
		return this;
	}

	@ConfigurationProperties(value = "spring.yarn.internal.YarnKillApplication")
	public static class OperationProperties {
		String applicationId;
		public void setApplicationId(String applicationId) {
			this.applicationId = applicationId;
		}
		public String getApplicationId() {
			return applicationId;
		}
	}

}
