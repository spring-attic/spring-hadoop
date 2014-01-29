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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.data.hadoop.config.common.annotation.AbstractConfiguredAnnotationBuilder;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.container.AbstractYarnContainer;
import org.springframework.yarn.container.YarnContainer;
import org.springframework.yarn.container.YarnContainerFactoryBean;

public class YarnContainerBuilder
		extends AbstractConfiguredAnnotationBuilder<YarnContainer, YarnContainerConfigurer, YarnContainerBuilder>
		implements YarnContainerConfigurer {

	private final static Log log = LogFactory.getLog(YarnContainerBuilder.class);

	private Class<? extends YarnContainer> clazz;
	private Configuration configuration;

	private YarnContainer ref;

	public YarnContainerBuilder() {
	}

	@Override
	protected YarnContainer performBuild() throws Exception {
		YarnContainerFactoryBean fb = new YarnContainerFactoryBean();
		fb.setContainerRef(ref);
		fb.setContainerClass(clazz);
		fb.afterPropertiesSet();
		YarnContainer container = fb.getObject();
		if (container instanceof AbstractYarnContainer) {
			((AbstractYarnContainer)container).setConfiguration(configuration);
		}
		return container;
	}

	@Override
	public YarnContainerConfigurer containerClass(Class<? extends YarnContainer> clazz) {
		log.info("Setting as class reference " + clazz);
		this.clazz = clazz;
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public YarnContainerConfigurer containerClass(String clazz) {
		log.info("Setting from a class name reference " + clazz);
		// let null or empty to pass without errors
		if (!StringUtils.hasText(clazz)) {
			return this;
		}

		Class<?> resolvedClass = ClassUtils.resolveClassName(clazz, getClass().getClassLoader());
		if (ClassUtils.isAssignable(YarnContainer.class, resolvedClass)) {
			this.clazz = (Class<? extends YarnContainer>) resolvedClass;
		} else {
			throw new IllegalArgumentException("Class " + resolvedClass + " is not an instance of YarnContainer");
		}
		return this;
	}

	public void configuration(Configuration configuration) {
		this.configuration = configuration;
	}

	public YarnContainerConfigurer containerRef(YarnContainer ref) {
		this.ref = ref;
		return this;
	}

}
