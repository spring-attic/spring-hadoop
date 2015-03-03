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
package org.springframework.data.hadoop.store.config.annotation.configuration;

import java.io.Closeable;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.data.hadoop.HadoopSystemConstants;
import org.springframework.data.hadoop.config.common.annotation.AbstractImportingAnnotationConfiguration;
import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurer;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.PartitionDataStoreWriter;
import org.springframework.data.hadoop.store.config.annotation.EnableDataStorePartitionTextWriter;
import org.springframework.data.hadoop.store.config.annotation.EnableDataStoreTextWriter;
import org.springframework.data.hadoop.store.config.annotation.SpringDataStoreWriterConfigs;
import org.springframework.data.hadoop.store.config.annotation.builders.SpringDataStoreTextWriterBuilder;
import org.springframework.data.hadoop.store.output.PartitionTextFileWriter;
import org.springframework.data.hadoop.store.output.TextFileWriter;
import org.springframework.data.hadoop.store.support.LifecycleObjectSupport;
import org.springframework.util.ClassUtils;

@Configuration
public class SpringDataStoreTextWriterConfiguration extends
		AbstractImportingAnnotationConfiguration<SpringDataStoreTextWriterBuilder, SpringDataStoreWriterConfigs> {

	private final SpringDataStoreTextWriterBuilder builder = new SpringDataStoreTextWriterBuilder();

	@Override
	protected BeanDefinition buildBeanDefinition(AnnotationMetadata importingClassMetadata,
			Class<? extends Annotation> namedAnnotation) throws Exception {
		BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(TextWriterDelegatingFactoryBean.class);
		beanDefinitionBuilder.addConstructorArgValue(builder);
		if (namedAnnotation == EnableDataStorePartitionTextWriter.class) {
			beanDefinitionBuilder.addConstructorArgValue(PartitionDataStoreWriter.class);
		} else {
			beanDefinitionBuilder.addConstructorArgValue(DataStoreWriter.class);
		}
		beanDefinitionBuilder.addConstructorArgValue(importingClassMetadata.getClassName());
		return beanDefinitionBuilder.getBeanDefinition();
	}

	@Override
	protected List<Class<? extends Annotation>> getAnnotations() {
		List<Class<? extends Annotation>> types = new ArrayList<Class<? extends Annotation>>();
		types.add(EnableDataStoreTextWriter.class);
		types.add(EnableDataStorePartitionTextWriter.class);
		return types;
	}

	private static class TextWriterDelegatingFactoryBean
			extends
			BeanDelegatingFactoryBean<DataStoreWriter<?>, SpringDataStoreTextWriterBuilder, SpringDataStoreWriterConfigs> implements SmartLifecycle, Closeable {

		private String clazzName;

		@Autowired(required = false)
		@Qualifier(HadoopSystemConstants.DEFAULT_ID_CONFIGURATION)
		private org.apache.hadoop.conf.Configuration configuration;

		// for calling lifecycle from this factory bean
		private LifecycleObjectSupport lifecycle;
		private Closeable closeable;

		public TextWriterDelegatingFactoryBean(SpringDataStoreTextWriterBuilder builder, Class<DataStoreWriter<?>> clazz,
				String clazzName) {
			super(builder, clazz);
			this.clazzName = clazzName;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public void afterPropertiesSet() throws Exception {
			for (AnnotationConfigurer<SpringDataStoreWriterConfigs, SpringDataStoreTextWriterBuilder> configurer : getConfigurers()) {
				Class<?> clazz = configurer.getClass();
				if (ClassUtils.getUserClass(clazz).getName().equals(clazzName)) {
					getBuilder().apply(configurer);
				}
			}

			SpringDataStoreWriterConfigs configs = getBuilder().getOrBuild();
			if (configs.getPartitionStrategy() == null) {
				TextFileWriter writer = new TextFileWriter(configuration != null ? configuration
						: configs.getConfiguration(), configs.getBasePath(), configs.getCodec());
				if (configs.getOverwrite() != null) {
					writer.setOverwrite(configs.getOverwrite());
				}
				if (configs.getAppendable() != null) {
					writer.setAppendable(configs.getAppendable());
				}
				if (configs.getInWritingPrefix() != null) {
					writer.setInWritingPrefix(configs.getInWritingPrefix());
				}
				if (configs.getInWritingSuffix() != null) {
					writer.setInWritingSuffix(configs.getInWritingSuffix());
				}
				if (configs.getIdleTimeout() != null) {
					writer.setIdleTimeout(configs.getIdleTimeout());
				}
				if (configs.getCloseTimeout() != null) {
					writer.setCloseTimeout(configs.getCloseTimeout());
				}
				if (configs.getFileOpenAttempts() != null) {
					writer.setMaxOpenAttempts(configs.getFileOpenAttempts());
				}
				if (configs.getFileNamingStrategy() != null) {
					writer.setFileNamingStrategy(configs.getFileNamingStrategy());
				}
				if (configs.getRolloverStrategy() != null) {
					writer.setRolloverStrategy(configs.getRolloverStrategy());
				}
				writer.setBeanFactory(getBeanFactory());
				writer.afterPropertiesSet();
				lifecycle = writer;
				closeable = writer;
				setObject(writer);
			} else {
				PartitionTextFileWriter writer = new PartitionTextFileWriter(configuration != null ? configuration
						: configs.getConfiguration(), configs.getBasePath(), configs.getCodec(),
						configs.getPartitionStrategy());
				if (configs.getOverwrite() != null) {
					writer.setOverwrite(configs.getOverwrite());
				}
				if (configs.getAppendable() != null) {
					writer.setAppendable(configs.getAppendable());
				}
				if (configs.getInWritingPrefix() != null) {
					writer.setInWritingPrefix(configs.getInWritingPrefix());
				}
				if (configs.getInWritingSuffix() != null) {
					writer.setInWritingSuffix(configs.getInWritingSuffix());
				}
				if (configs.getIdleTimeout() != null) {
					writer.setIdleTimeout(configs.getIdleTimeout());
				}
				if (configs.getCloseTimeout() != null) {
					writer.setCloseTimeout(configs.getCloseTimeout());
				}
				if (configs.getFileOpenAttempts() != null) {
					writer.setMaxOpenAttempts(configs.getFileOpenAttempts());
				}
				if (configs.getFileNamingStrategy() != null) {
					writer.setFileNamingStrategyFactory(configs.getFileNamingStrategy());
				}
				if (configs.getRolloverStrategy() != null) {
					writer.setRolloverStrategyFactory(configs.getRolloverStrategy());
				}
				writer.setBeanFactory(getBeanFactory());
				writer.afterPropertiesSet();
				lifecycle = writer;
				closeable = writer;
				setObject(writer);
			}
		}

		@Override
		public void start() {
			lifecycle.start();
		}

		@Override
		public void stop() {
			lifecycle.stop();
		}

		@Override
		public boolean isRunning() {
			return lifecycle.isRunning();
		}

		@Override
		public int getPhase() {
			return lifecycle.getPhase();
		}

		@Override
		public boolean isAutoStartup() {
			return lifecycle.isAutoStartup();
		}

		@Override
		public void stop(Runnable callback) {
			lifecycle.stop(callback);
		}

		@Override
		public void close() throws IOException {
			closeable.close();
		}

	}

}
