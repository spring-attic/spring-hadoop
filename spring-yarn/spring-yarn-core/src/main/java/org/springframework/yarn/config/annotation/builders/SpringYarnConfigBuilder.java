/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.yarn.config.annotation.builders;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.data.hadoop.config.common.annotation.AbstractConfiguredAnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.AnnotationBuilder;
import org.springframework.yarn.config.annotation.SpringYarnConfigs;
import org.springframework.yarn.fs.ResourceLocalizer;
import org.springframework.yarn.support.YarnUtils;

/**
 * {@link AnnotationBuilder} for {@link SpringYarnConfigs}.
 *
 * @author Janne Valkealahti
 *
 */
public class SpringYarnConfigBuilder
		extends AbstractConfiguredAnnotationBuilder<SpringYarnConfigs,SpringYarnConfigBuilder,SpringYarnConfigBuilder> {

	private final static Log log = LogFactory.getLog(SpringYarnConfigBuilder.class);

	/** Mostly used by allowing config to be injected i.e from tests */
	private Configuration yarnConfiguration;

	/**
	 * Instantiates a new spring yarn config builder.
	 */
	public SpringYarnConfigBuilder() {}

	@Override
	protected SpringYarnConfigs performBuild() throws Exception {
		SpringYarnConfigs config = new SpringYarnConfigs();

		// shared objects are created in SpringYarnConfigurerAdapter
		YarnConfigBuilder sharedObject = getSharedObject(YarnConfigBuilder.class);

		log.info("Existing yarnConfiguration: " + YarnUtils.toString(yarnConfiguration));

		Configuration buildConfiguration = getSharedObject(YarnConfigBuilder.class).build();

		// TODO: we should find better way to merge configs
		Configuration configuration = (yarnConfiguration == null)
				? buildConfiguration
				: YarnUtils.merge(buildConfiguration, yarnConfiguration);

		if (log.isDebugEnabled()) {
			log.debug("YarnConfigBuilder shared: " + sharedObject);
			log.debug("Existing buildConfiguration: " + YarnUtils.toString(buildConfiguration));
		}
		log.info("Setting configuration for SpringYarnConfigs: " + YarnUtils.toString(configuration));


		config.setConfiguration(configuration);

		YarnResourceLocalizerBuilder yarnResourceLocalizerBuilder = getSharedObject(YarnResourceLocalizerBuilder.class);
		yarnResourceLocalizerBuilder.configuration(configuration);
		ResourceLocalizer localizer = yarnResourceLocalizerBuilder.build();
		config.setLocalizer(localizer);

		YarnEnvironmentBuilder yarnEnvironmentBuilder = getSharedObject(YarnEnvironmentBuilder.class);
		yarnEnvironmentBuilder.configuration(configuration);
		Map<String, Map<String, String>> envs = yarnEnvironmentBuilder.build();
		Map<String, String> env = envs.get(null);
		config.setEnvironment(env);

		YarnClientBuilder yarnClientBuilder = getSharedObject(YarnClientBuilder.class);
		if (yarnClientBuilder != null) {
			yarnClientBuilder.configuration(configuration);
			yarnClientBuilder.setResourceLocalizer(localizer);
			yarnClientBuilder.setEnvironment(env);
			config.setYarnClient(yarnClientBuilder.build());
		}

		YarnAppmasterBuilder yarnAppmasterBuilder = getSharedObject(YarnAppmasterBuilder.class);
		if (yarnAppmasterBuilder != null) {
			yarnAppmasterBuilder.configuration(configuration);
			yarnAppmasterBuilder.setResourceLocalizer(localizer);
			yarnAppmasterBuilder.setEnvironment(env);
			yarnAppmasterBuilder.setEnvironments(envs);
			config.setYarnAppmaster(yarnAppmasterBuilder.build());
		}

		YarnContainerBuilder yarnContainerBuilder = getSharedObject(YarnContainerBuilder.class);
		if (yarnContainerBuilder != null) {
			yarnContainerBuilder.configuration(configuration);
			config.setYarnContainer(yarnContainerBuilder.build());
		}

		return config;
	}

	/**
	 * Sets the yarn configuration.
	 *
	 * @param yarnConfiguration the new yarn configuration
	 */
	public void setYarnConfiguration(Configuration yarnConfiguration) {
		this.yarnConfiguration = yarnConfiguration;
	}

}
