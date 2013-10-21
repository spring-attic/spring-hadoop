/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.hadoop.fs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.support.ResourceEditorRegistrar;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ResourceEditor;
import org.springframework.core.io.support.ResourcePatternResolver;

/**
 * Utility class that overrides the built-in {@link ResourceEditor} to allow
 * {@link HdfsResourceLoader} to be searched first. Also sets {@code ResourceLoader}
 * for the Application context.
 *
 * @author Costin Leau
 * @author Janne Valkealahti
 *
 */
public class CustomResourceLoaderRegistrar implements ApplicationContextAware, BeanFactoryPostProcessor, Ordered {

	private final static Log log = LogFactory.getLog(CustomResourceLoaderRegistrar.class);

	private ResourcePatternResolver loader;

	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
		// this enables resolving from xml
		log.info("Adding PropertyEditorRegistrar");
		beanFactory.addPropertyEditorRegistrar(new ResourceEditorRegistrar(loader, new StandardEnvironment()));
	}

	@Override
	public int getOrder() {
		return Ordered.LOWEST_PRECEDENCE;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		// We set the context resource loader here
		if (applicationContext instanceof GenericApplicationContext) {
			log.info("Setting context resource loader");
			((GenericApplicationContext) applicationContext).setResourceLoader(loader);
		}
	}

	/**
	 * Sets the resource pattern loader.
	 *
	 * @param loader the new resource pattern loader
	 */
	public void setLoader(ResourcePatternResolver loader) {
		this.loader = loader;
	}

}