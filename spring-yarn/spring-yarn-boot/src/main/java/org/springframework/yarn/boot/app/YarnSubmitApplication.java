/*
 * Copyright 2014-2015 the original author or authors.
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
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.EndpointMBeanExportAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.web.EmbeddedServletContainerAutoConfiguration;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.yarn.boot.SpringApplicationCallback;
import org.springframework.yarn.boot.SpringApplicationTemplate;
import org.springframework.yarn.boot.properties.SpringYarnProperties;
import org.springframework.yarn.boot.support.SpringYarnBootUtils;
import org.springframework.yarn.client.ApplicationDescriptor;
import org.springframework.yarn.client.ApplicationYarnClient;
import org.springframework.yarn.client.YarnClient;

/**
 * Generic Spring Boot client application used to submit Spring Yarn Boot based apps into Yarn.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@EnableAutoConfiguration(exclude = { EmbeddedServletContainerAutoConfiguration.class, WebMvcAutoConfiguration.class,
		JmxAutoConfiguration.class, BatchAutoConfiguration.class, JmxAutoConfiguration.class,
		EndpointMBeanExportAutoConfiguration.class, EndpointAutoConfiguration.class })
public class YarnSubmitApplication extends AbstractClientApplication<ApplicationId, YarnSubmitApplication> {

	private String applicationName;
	
	/**
	 * Run a {@link SpringApplication} build by a
	 * {@link SpringApplicationBuilder} using an empty args.
	 *
	 * @return the application id
	 * @see #run(String...)
	 */
	public ApplicationId run() {
		return run(new String[0]);
	}

	/**
	 * Run a {@link SpringApplication} build by a
	 * {@link SpringApplicationBuilder} using an empty args.
	 *
	 * @param args the args
	 * @return the application id
	 */
	public ApplicationId run(String... args) {
		Assert.state(StringUtils.hasText(applicationVersion), "Instance id must be set");
		SpringApplicationBuilder builder = new SpringApplicationBuilder();
		builder.web(false);
		builder.sources(YarnSubmitApplication.class);
		SpringYarnBootUtils.addSources(builder, sources.toArray(new Object[0]));
		SpringYarnBootUtils.addProfiles(builder, profiles.toArray(new String[0]));

		if (StringUtils.hasText(applicationBaseDir)) {
			appProperties.setProperty("spring.yarn.applicationDir", applicationBaseDir + applicationVersion + "/");
		}

		appProperties.setProperty("spring.yarn.applicationVersion", applicationVersion);
		SpringYarnBootUtils.addApplicationListener(builder, appProperties);

		SpringApplicationTemplate template = new SpringApplicationTemplate(builder);
		return template.execute(new SpringApplicationCallback<ApplicationId>() {

			@Override
			public ApplicationId runWithSpringApplication(ApplicationContext context) throws Exception {
				YarnClient client = context.getBean(YarnClient.class);
				SpringYarnProperties syp = context.getBean(SpringYarnProperties.class);
				String applicationdir = SpringYarnBootUtils.resolveApplicationdir(syp);
				if (client instanceof ApplicationYarnClient) {
					return ((ApplicationYarnClient)client).submitApplication(new ApplicationDescriptor(applicationdir, applicationName));
				} else {
					return client.submitApplication(false);
				}
			}

		}, args);
	}

	@Override
	protected YarnSubmitApplication getThis() {
		return this;
	}

	/**
	 * Sets the application name used for submit. Effectively this will override
	 * setting from set for yarn client for configuration properties.
	 * 
	 * @param applicationName the application name 
	 * @return the YarnSubmitApplication for chaining
	 */
	public YarnSubmitApplication applicationName(String applicationName) {
		this.applicationName = applicationName;
		return getThis();
	}

}
