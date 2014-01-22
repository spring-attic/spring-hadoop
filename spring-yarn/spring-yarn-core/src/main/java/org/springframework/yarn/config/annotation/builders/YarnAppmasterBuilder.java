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
import org.springframework.yarn.am.allocate.DefaultContainerAllocator;
import org.springframework.yarn.am.container.DefaultContainerLauncher;
import org.springframework.yarn.am.monitor.DefaultContainerMonitor;
import org.springframework.yarn.config.annotation.configurers.MasterContainerRunnerConfigurer;
import org.springframework.yarn.fs.ResourceLocalizer;

/**
 *
 *
 * @author Janne Valkealahti
 *
 */
public final class YarnAppmasterBuilder extends AbstractConfiguredAnnotationBuilder<YarnAppmaster, YarnAppmasterConfigure, YarnAppmasterBuilder>
		implements YarnAppmasterConfigure {

	/** Appmaster class, defaults to StaticAppmaster */
	private Class<? extends YarnAppmaster> appmasterClazz = StaticAppmaster.class;

	private Configuration configuration;
	private ResourceLocalizer resourceLocalizer;
	private Map<String, String> environment;
	private String[] commands;

	public YarnAppmasterBuilder() {
		super();
	}

	public YarnAppmasterBuilder(ObjectPostProcessor<Object> objectPostProcessor) {
		super(objectPostProcessor);
	}

	@Override
	protected YarnAppmaster performBuild() throws Exception {

		YarnAppmaster appmaster = BeanUtils.instantiate(appmasterClazz);

		if (appmaster instanceof AbstractAppmaster) {
			AbstractAppmaster abstractAppmaster = (AbstractAppmaster) appmaster;
			if (commands != null) {
				abstractAppmaster.setCommands(commands);
			}

			abstractAppmaster.setConfiguration(configuration);
			abstractAppmaster.setEnvironment(environment);
			abstractAppmaster.setResourceLocalizer(resourceLocalizer);
			if (appmaster instanceof AbstractServicesAppmaster) {
				AbstractServicesAppmaster abstractServicesAppmaster = (AbstractServicesAppmaster)appmaster;

				DefaultContainerLauncher launcher = new DefaultContainerLauncher();
				launcher.setConfiguration(configuration);
				launcher.setEnvironment(environment);
				launcher.setResourceLocalizer(resourceLocalizer);
				abstractServicesAppmaster.setLauncher(launcher);

				DefaultContainerAllocator allocator = new DefaultContainerAllocator();
				allocator.setConfiguration(configuration);
				allocator.setEnvironment(environment);
				abstractServicesAppmaster.setAllocator(postProcess(allocator));

				abstractServicesAppmaster.setMonitor(new DefaultContainerMonitor());
			}

		}
		return appmaster;
	}

	public MasterContainerRunnerConfigurer withContainerRunner() throws Exception {
		return apply(new MasterContainerRunnerConfigurer());
	}

	public void configuration(Configuration configuration) {
		this.configuration = configuration;
	}

	public void setResourceLocalizer(ResourceLocalizer resourceLocalizer) {
		this.resourceLocalizer = resourceLocalizer;
	}

	public void setEnvironment(Map<String, String> environment) {
		this.environment = environment;
	}

	@Override
	public YarnAppmasterBuilder clazz(Class<? extends YarnAppmaster> clazz) {
		appmasterClazz = clazz;
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public YarnAppmasterBuilder clazz(String clazz) {
		// let null or empty to pass without errors
		if (!StringUtils.hasText(clazz)) {
			return this;
		}

		Class<?> resolvedClass = ClassUtils.resolveClassName(clazz, getClass().getClassLoader());
		if (ClassUtils.isAssignable(YarnAppmaster.class, resolvedClass)) {
			appmasterClazz = (Class<? extends YarnAppmaster>) resolvedClass;
		} else {
			throw new IllegalArgumentException("Class " + resolvedClass + " is not an instance of YarnAppmaster");
		}
		return this;
	}

	@Override
	public YarnAppmasterBuilder containerCommands(String... commands) {
		this.commands = commands;
		return this;
	}

}
