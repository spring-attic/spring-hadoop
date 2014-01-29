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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.hadoop.config.common.annotation.AnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.ObjectPostProcessor;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.builders.SpringYarnConfigBuilder;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterBuilder;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnClientBuilder;
import org.springframework.yarn.config.annotation.builders.YarnClientConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnConfigBuilder;
import org.springframework.yarn.config.annotation.builders.YarnConfigConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnContainerBuilder;
import org.springframework.yarn.config.annotation.builders.YarnContainerConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentBuilder;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnResourceLocalizerBuilder;
import org.springframework.yarn.config.annotation.builders.YarnResourceLocalizerConfigurer;

/**
 * Provides a convenient base class for creating a {@link SpringYarnConfigurer}
 * instance. The implementation allows customization by overriding methods.
 *
 * @author Janne Valkealahti
 * @see EnableYarn
 *
 */
public class SpringYarnConfigurerAdapter implements SpringYarnConfigurer {

	private final static Log log = LogFactory.getLog(SpringYarnConfigurerAdapter.class);

	private YarnConfigBuilder yarnConfigBuilder;
	private YarnResourceLocalizerBuilder yarnResourceLocalizerBuilder;
	private YarnEnvironmentBuilder yarnEnvironmentBuilder;
	private YarnClientBuilder yarnClientBuilder;
	private YarnAppmasterBuilder yarnAppmasterBuilder;
	private YarnContainerBuilder yarnContainerBuilder;

	private ObjectPostProcessor<Object> objectPostProcessor = new ObjectPostProcessor<Object>() {
		@Override
		public <T> T postProcess(T object) {
			throw new IllegalStateException(ObjectPostProcessor.class.getName()
					+ " is a required bean. Ensure you have used @EnableYarn and @Configuration");
		}
	};

	@Autowired(required=false)
	public void setObjectPostProcessor(ObjectPostProcessor<Object> objectPostProcessor) {
		this.objectPostProcessor = objectPostProcessor;
	}

	@Override
	public final void init(SpringYarnConfigBuilder builder) throws Exception {
		builder.setSharedObject(YarnConfigBuilder.class, getConfigBuilder());
		builder.setSharedObject(YarnResourceLocalizerBuilder.class, getLocalizerBuilder());
		builder.setSharedObject(YarnEnvironmentBuilder.class, getEnvironmentBuilder());

		EnableYarn annotation = AnnotationUtils.findAnnotation(getClass(), EnableYarn.class);
		Enable enable = annotation.enable();

		if (log.isDebugEnabled()) {
			log.debug("Enabling builder for " + enable);
		}

		if (enable == Enable.CLIENT) {
			builder.setSharedObject(YarnClientBuilder.class, getClientBuilder());
		} else if (enable == Enable.APPMASTER) {
			builder.setSharedObject(YarnAppmasterBuilder.class, getAppmasterBuilder());
		} else if (enable == Enable.CONTAINER) {
			builder.setSharedObject(YarnContainerBuilder.class, getContainerBuilder());
		}
	}

	@Override
	public void configure(SpringYarnConfigBuilder builder) throws Exception {
	}

	@Override
	public void configure(YarnConfigConfigurer config) throws Exception {
	}

	@Override
	public void configure(YarnResourceLocalizerConfigurer localizer) throws Exception {
	}

	@Override
	public void configure(YarnEnvironmentConfigurer environment) throws Exception {
	}

	@Override
	public void configure(YarnClientConfigurer client) throws Exception {
	}

	@Override
	public void configure(YarnAppmasterConfigurer master) throws Exception {
	}

	@Override
	public void configure(YarnContainerConfigurer container) throws Exception {
	}

	/**
	 * Gets the Yarn config builder.
	 *
	 * @return the Yarn config builder
	 * @throws Exception if error occurred
	 */
	protected final YarnConfigBuilder getConfigBuilder() throws Exception {
		if (yarnConfigBuilder != null) {
			return yarnConfigBuilder;
		}
		yarnConfigBuilder = new YarnConfigBuilder(objectPostProcessor);
		configure(yarnConfigBuilder);
		return yarnConfigBuilder;
	}

	protected final YarnResourceLocalizerBuilder getLocalizerBuilder() throws Exception {
		if (yarnResourceLocalizerBuilder != null) {
			return yarnResourceLocalizerBuilder;
		}
		yarnResourceLocalizerBuilder = new YarnResourceLocalizerBuilder();
		configure(yarnResourceLocalizerBuilder);
		return yarnResourceLocalizerBuilder;
	}

	protected final YarnEnvironmentBuilder getEnvironmentBuilder() throws Exception {
		if (yarnEnvironmentBuilder != null) {
			return yarnEnvironmentBuilder;
		}
		yarnEnvironmentBuilder = new YarnEnvironmentBuilder();
		configure(yarnEnvironmentBuilder);
		return yarnEnvironmentBuilder;
	}

	protected final YarnClientBuilder getClientBuilder() throws Exception {
		if (yarnClientBuilder != null) {
			return yarnClientBuilder;
		}
		yarnClientBuilder = new YarnClientBuilder();
		configure(yarnClientBuilder);
		return yarnClientBuilder;
	}

	protected final YarnAppmasterBuilder getAppmasterBuilder() throws Exception {
		if (yarnAppmasterBuilder != null) {
			return yarnAppmasterBuilder;
		}
		yarnAppmasterBuilder = new YarnAppmasterBuilder(objectPostProcessor);
		configure(yarnAppmasterBuilder);
		return yarnAppmasterBuilder;
	}

	protected final YarnContainerBuilder getContainerBuilder() throws Exception {
		if (yarnContainerBuilder != null) {
			return yarnContainerBuilder;
		}
		yarnContainerBuilder = new YarnContainerBuilder();
		configure(yarnContainerBuilder);
		return yarnContainerBuilder;
	}

	@Override
	public boolean isAssignable(AnnotationBuilder<SpringYarnConfigs> builder) {
		return true;
	}

}
