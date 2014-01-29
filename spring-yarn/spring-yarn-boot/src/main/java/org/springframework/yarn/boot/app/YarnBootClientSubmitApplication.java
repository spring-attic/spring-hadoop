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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.web.EmbeddedServletContainerAutoConfiguration;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.boot.SpringApplicationCallback;
import org.springframework.yarn.boot.SpringApplicationTemplate;
import org.springframework.yarn.boot.support.SpringYarnBootUtils;
import org.springframework.yarn.client.YarnClient;

/**
 * Generic Spring Boot client application used to submit Spring Yarn Boot based apps into Yarn.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@EnableAutoConfiguration(exclude = { EmbeddedServletContainerAutoConfiguration.class, WebMvcAutoConfiguration.class,
		JmxAutoConfiguration.class, BatchAutoConfiguration.class })
public class YarnBootClientSubmitApplication {

	private String appId;
	private List<Object> sources = new ArrayList<Object>();
	private List<String> profiles = new ArrayList<String>();
	private Properties appProperties;


	public YarnBootClientSubmitApplication appId(String appId) {
		Assert.state(StringUtils.hasText(appId), "App id must not be empty");
		this.appId = appId;
		return this;
	}

	public YarnBootClientSubmitApplication profiles(String ... profiles) {
		if (!ObjectUtils.isEmpty(profiles)) {
			this.profiles.addAll(Arrays.asList(profiles));
		}
		return this;
	}

	public YarnBootClientSubmitApplication sources(Object... sources) {
		if (!ObjectUtils.isEmpty(sources)) {
			this.sources.addAll(Arrays.asList(sources));
		}
		return this;
	}

	public YarnBootClientSubmitApplication appProperties(Properties appProperties) {
		this.appProperties = appProperties;
		return this;
	}

	public ApplicationId run(String... args) {
		Assert.state(StringUtils.hasText(appId), "App id must be set");
		SpringApplicationBuilder builder = new SpringApplicationBuilder();
		builder.web(false);
		builder.sources(YarnBootClientSubmitApplication.class);
		SpringYarnBootUtils.addSources(builder, sources.toArray(new Object[0]));
		SpringYarnBootUtils.addProfiles(builder, profiles.toArray(new String[0]));

		SpringYarnBootUtils.addApplicationListener(builder, appProperties);

		SpringApplicationTemplate template = new SpringApplicationTemplate(builder);
		return template.execute(new SpringApplicationCallback<ApplicationId>() {

			@Override
			public ApplicationId runWithSpringApplication(ApplicationContext context) throws Exception {
				YarnClient client = context.getBean(YarnClient.class);
				return client.submitApplication(false);
			}

		}, args);
	}

}
