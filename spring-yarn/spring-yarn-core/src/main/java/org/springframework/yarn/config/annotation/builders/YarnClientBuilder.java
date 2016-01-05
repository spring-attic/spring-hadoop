/*
 * Copyright 2013-2016 the original author or authors.
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

import org.apache.hadoop.conf.Configuration;
import org.springframework.data.hadoop.config.common.annotation.AbstractConfiguredAnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.AnnotationBuilder;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.client.YarnClientFactoryBean;
import org.springframework.yarn.config.annotation.configurers.DefaultClientMasterRunnerConfigurer;
import org.springframework.yarn.fs.ResourceLocalizer;
import org.springframework.yarn.support.ParsingUtils;

/**
 * {@link AnnotationBuilder} for {@link YarnClient}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnClientBuilder
		extends AbstractConfiguredAnnotationBuilder<YarnClient, YarnClientConfigurer, YarnClientBuilder>
		implements YarnClientConfigurer {

	private Configuration configuration;
	private ResourceLocalizer resourceLocalizer;
	private Map<String, String> environment;
	private String appName;
	private String appType;
	private String[] commands;
	private Integer priority;
	private String queue;
	private String labelExpression;
	private String memory;
	private Integer virtualCores;
	private Class<? extends YarnClient> clientClass;

	/**
	 * Instantiates a new yarn client builder.
	 */
	public YarnClientBuilder() {}

	@Override
	protected YarnClient performBuild() throws Exception {
		YarnClientFactoryBean fb = new YarnClientFactoryBean();
		fb.setConfiguration(configuration);
		fb.setResourceLocalizer(resourceLocalizer);
		fb.setEnvironment(environment);
		fb.setAppName(appName);
		fb.setAppType(appType);
		if (clientClass != null) {
			fb.setClientClass(clientClass);
		}
		if (commands != null) {
			fb.setCommands(commands);
		}
		if (priority != null) {
			fb.setPriority(priority);
		}
		if (queue != null) {
			fb.setQueue(queue);
		}
		if (labelExpression != null) {
			fb.setLabelExpression(labelExpression);
		}
		if (virtualCores != null) {
			fb.setVirtualcores(virtualCores);
		}
		if (memory != null) {
			fb.setMemory(ParsingUtils.parseBytesAsMegs(memory));
		}
		fb.afterPropertiesSet();
		return fb.getObject();
	}

	/**
	 * Add commands for starting {@link YarnAppmaster}.
	 *
	 * @return the client master runner configurer
	 * @throws Exception the exception
	 */
	@Override
	public DefaultClientMasterRunnerConfigurer withMasterRunner() throws Exception {
		return apply(new DefaultClientMasterRunnerConfigurer());
	}

	@Override
	public YarnClientConfigurer appName(String appName) {
		this.appName = appName;
		return this;
	}

	@Override
	public YarnClientConfigurer appType(String appType) {
		this.appType = appType;
		return this;
	}

	@Override
	public YarnClientConfigurer masterCommands(String... commands) {
		this.commands = commands;
		return this;
	}

	@Override
	public YarnClientConfigurer priority(Integer priority) {
		this.priority = priority;
		return this;
	}

	@Override
	public YarnClientConfigurer queue(String queue) {
		this.queue = queue;
		return this;
	}

	@Override
	public YarnClientConfigurer labelExpression(String labelExpression) {
		this.labelExpression= labelExpression;
		return this;
	}

	@Override
	public YarnClientConfigurer memory(int memory) {
		this.memory = Integer.toString(memory);
		return this;
	}

	@Override
	public YarnClientConfigurer memory(String memory) {
		this.memory = memory;
		return this;
	}

	@Override
	public YarnClientConfigurer virtualCores(Integer virtualCores) {
		this.virtualCores = virtualCores;
		return this;
	}

	@Override
	public YarnClientConfigurer clientClass(Class<? extends YarnClient> clazz) {
		this.clientClass = clazz;
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public YarnClientConfigurer clientClass(String clazz) {
		// let null or empty to pass without errors
		if (!StringUtils.hasText(clazz)) {
			return this;
		}

		Class<?> resolvedClass = ClassUtils.resolveClassName(clazz, getClass().getClassLoader());
		if (ClassUtils.isAssignable(YarnClient.class, resolvedClass)) {
			clientClass = (Class<? extends YarnClient>) resolvedClass;
		} else {
			throw new IllegalArgumentException("Class " + resolvedClass + " is not an instance of YarnClient");
		}
		return this;
	}

	public void configuration(Configuration configuration) {
		this.configuration = configuration;
	}

	public void setCommands(String... commands) {
		this.commands = commands;
	}

	public void setResourceLocalizer(ResourceLocalizer resourceLocalizer) {
		this.resourceLocalizer = resourceLocalizer;
	}

	public void setEnvironment(Map<String, String> environment) {
		this.environment = environment;
	}

}
