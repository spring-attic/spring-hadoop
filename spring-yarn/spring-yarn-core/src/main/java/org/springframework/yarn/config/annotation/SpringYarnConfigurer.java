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
package org.springframework.yarn.config.annotation;

import java.util.Map;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.data.hadoop.config.common.annotation.AnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurer;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.config.annotation.builders.SpringYarnConfigBuilder;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterBuilder;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterConfigure;
import org.springframework.yarn.config.annotation.builders.YarnClientBuilder;
import org.springframework.yarn.config.annotation.builders.YarnClientConfigure;
import org.springframework.yarn.config.annotation.builders.YarnConfigBuilder;
import org.springframework.yarn.config.annotation.builders.YarnConfigConfigure;
import org.springframework.yarn.config.annotation.builders.YarnContainerBuilder;
import org.springframework.yarn.config.annotation.builders.YarnContainerConfigure;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentBuilder;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentConfigure;
import org.springframework.yarn.config.annotation.builders.YarnResourceLocalizerBuilder;
import org.springframework.yarn.config.annotation.builders.YarnResourceLocalizerConfigure;
import org.springframework.yarn.container.YarnContainer;
import org.springframework.yarn.fs.ResourceLocalizer;

/**
 * Allows for configuring a {@link AnnotationBuilder}. All
 * {@link AnnotationConfigurer} first have their {@link #init(AnnotationBuilder)}
 * method invoked. After all {@link #init(AnnotationBuilder)} methods have been
 * invoked, each {@link #configure(AnnotationBuilder)} method is invoked.
 *
 * @author Janne Valkealahti
 *
 */
public interface SpringYarnConfigurer extends AnnotationConfigurer<SpringYarnConfigs, SpringYarnConfigBuilder> {

	/**
	 * Configure {@link YarnConfiguration} via {@link YarnConfigBuilder} builder.
	 *
	 * @param config the {@link YarnConfiguration} builder
	 * @throws Exception if error occurred
	 */
	void configure(YarnConfigConfigure config) throws Exception;

	/**
	 * Configure {@link ResourceLocalizer} via {@link YarnResourceLocalizerBuilder} builder.
	 *
	 * @param config the {@link ResourceLocalizer} builder
	 * @throws Exception if error occurred
	 */
	void configure(YarnResourceLocalizerConfigure localizer) throws Exception;

	/**
	 * Configure {@link Map} of environment via {@link YarnEnvironmentBuilder} builder.
	 *
	 * @param environment the {@link YarnEnvironmentBuilder} builder
	 * @throws Exception if error occurred
	 */
	void configure(YarnEnvironmentConfigure environment) throws Exception;

	/**
	 * Configure {@link YarnClient} via {@link YarnClientBuilder} builder.
	 *
	 * @param client the {@link YarnClientBuilder} builder
	 * @throws Exception if error occurred
	 */
	void configure(YarnClientConfigure client) throws Exception;

	/**
	 * Configure {@link YarnAppmaster} via {@link YarnAppmasterBuilder} builder.
	 *
	 * @param master the {@link YarnAppmasterBuilder} builder
	 * @throws Exception if error occurred
	 */
	void configure(YarnAppmasterConfigure master) throws Exception;

	/**
	 * Configure {@link YarnContainer} via {@link YarnContainerBuilder} builder.
	 *
	 * @param container the {@link YarnContainerBuilder} builder
	 * @throws Exception if error occurred
	 */
	void configure(YarnContainerConfigure container) throws Exception;

}
