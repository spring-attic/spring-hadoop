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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.BeanUtils;
import org.springframework.data.hadoop.config.common.annotation.AbstractConfiguredAnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.ObjectPostProcessor;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.am.AbstractAppmaster;
import org.springframework.yarn.am.AbstractServicesAppmaster;
import org.springframework.yarn.am.StaticAppmaster;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.allocate.AbstractAllocator;
import org.springframework.yarn.am.allocate.ContainerAllocator;
import org.springframework.yarn.am.allocate.DefaultContainerAllocator;
import org.springframework.yarn.am.container.DefaultContainerLauncher;
import org.springframework.yarn.am.monitor.DefaultContainerMonitor;
import org.springframework.yarn.config.annotation.configurers.DefaultMasterContainerAllocatorConfigurer;
import org.springframework.yarn.config.annotation.configurers.DefaultMasterContainerRunnerConfigurer;
import org.springframework.yarn.config.annotation.configurers.MasterContainerAllocatorConfigurer;
import org.springframework.yarn.config.annotation.configurers.MasterContainerRunnerConfigurer;
import org.springframework.yarn.fs.ResourceLocalizer;

/**
 *
 *
 * @author Janne Valkealahti
 *
 */
public final class YarnAppmasterBuilder extends AbstractConfiguredAnnotationBuilder<YarnAppmaster, YarnAppmasterConfigurer, YarnAppmasterBuilder>
		implements YarnAppmasterConfigurer {

	/** Appmaster class, defaults to StaticAppmaster */
	private Class<? extends YarnAppmaster> appmasterClass = StaticAppmaster.class;

	private Configuration configuration;
	private ResourceLocalizer resourceLocalizer;
	private ContainerAllocator containerAllocator;
	private Map<String, String> environment;
	private Map<String, Map<String, String>> environments = new HashMap<String, Map<String,String>>();
	private final Map<String, String[]> commands = new HashMap<String, String[]>();

	public YarnAppmasterBuilder() {
		super();
	}

	public YarnAppmasterBuilder(ObjectPostProcessor<Object> objectPostProcessor) {
		super(objectPostProcessor);
	}

	@Override
	protected YarnAppmaster performBuild() throws Exception {

		YarnAppmaster appmaster = BeanUtils.instantiateClass(appmasterClass);

		if (appmaster instanceof AbstractAppmaster) {
			AbstractAppmaster abstractAppmaster = (AbstractAppmaster) appmaster;
			for (Entry<String, String[]> entry : commands.entrySet()) {
				abstractAppmaster.setCommands(entry.getKey(), entry.getValue());
			}

			for (Entry<String, Map<String, String>> entry : environments.entrySet()) {
				abstractAppmaster.setEnvironment(entry.getKey(), entry.getValue());
			}

			abstractAppmaster.setConfiguration(configuration);
			abstractAppmaster.setResourceLocalizer(resourceLocalizer);
			if (appmaster instanceof AbstractServicesAppmaster) {
				AbstractServicesAppmaster abstractServicesAppmaster = (AbstractServicesAppmaster)appmaster;

				DefaultContainerLauncher launcher = new DefaultContainerLauncher();
				launcher.setConfiguration(configuration);
				launcher.setEnvironment(environment);
				launcher.setResourceLocalizer(resourceLocalizer);
				abstractServicesAppmaster.setLauncher(postProcess(launcher));

				if (containerAllocator == null) {
					containerAllocator = new DefaultContainerAllocator();
				}
				if (containerAllocator instanceof AbstractAllocator) {
					((AbstractAllocator)containerAllocator).setConfiguration(configuration);
					((AbstractAllocator)containerAllocator).setEnvironment(environment);
				}
				abstractServicesAppmaster.setAllocator(postProcess(containerAllocator));
				abstractServicesAppmaster.setMonitor(postProcess(new DefaultContainerMonitor()));
			}

		}
		return appmaster;
	}

	@Override
	public MasterContainerRunnerConfigurer withContainerRunner() throws Exception {
		return apply(new DefaultMasterContainerRunnerConfigurer());
	}

	@Override
	public MasterContainerAllocatorConfigurer withContainerAllocator() throws Exception {
		return apply(new DefaultMasterContainerAllocatorConfigurer());
	}

	public void configuration(Configuration configuration) {
		this.configuration = configuration;
	}

	public void setResourceLocalizer(ResourceLocalizer resourceLocalizer) {
		this.resourceLocalizer = resourceLocalizer;
	}

	public void setContainerAllocator(ContainerAllocator containerAllocator) {
		this.containerAllocator = containerAllocator;
	}

	public void setEnvironment(Map<String, String> environment) {
		this.environment = environment;
	}

	public void setEnvironments(Map<String, Map<String, String>> environments) {
		this.environments.putAll(environments);
	}

	@Override
	public YarnAppmasterBuilder appmasterClass(Class<? extends YarnAppmaster> clazz) {
		appmasterClass = clazz;
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public YarnAppmasterBuilder appmasterClass(String clazz) {
		// let null or empty to pass without errors
		if (!StringUtils.hasText(clazz)) {
			return this;
		}

		Class<?> resolvedClass = ClassUtils.resolveClassName(clazz, getClass().getClassLoader());
		if (ClassUtils.isAssignable(YarnAppmaster.class, resolvedClass)) {
			appmasterClass = (Class<? extends YarnAppmaster>) resolvedClass;
		} else {
			throw new IllegalArgumentException("Class " + resolvedClass + " is not an instance of YarnAppmaster");
		}
		return this;
	}

	@Override
	public YarnAppmasterBuilder containerCommands(String[] commands) {
		// null indicates a default value
		containerCommands(null, commands);
		return this;
	}

	@Override
	public YarnAppmasterBuilder containerCommands(String id, String[] commands) {
		this.commands.put(id, commands);
		return this;
	}

}
