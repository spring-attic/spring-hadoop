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

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.support.ResourceEditorRegistrar;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.Ordered;
import org.springframework.core.io.ResourceEditor;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.ResourcePatternResolver;

/**
 * Utility class that overrides the built-in {@link ResourceEditor} to allow {@link HdfsResourceLoader} to be
 * searched first.
 * 
 * @author Costin Leau
 */
public class CustomResourceLoaderRegistrar implements ApplicationContextAware, BeanFactoryPostProcessor, Ordered {

	private ChainedResourceLoader chain;
	private ResourcePatternResolver loader;

	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
		beanFactory.addPropertyEditorRegistrar(new ResourceEditorRegistrar(chain));
	}

	@Override
	public int getOrder() {
		return Ordered.LOWEST_PRECEDENCE;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		chain = new ChainedResourceLoader();
		chain.setFallback(loader);
		chain.setApplicationContext(applicationContext);

		Map<String, ResourceLoader> loaders = new LinkedHashMap<String, ResourceLoader>();
		loaders.put("classpath", applicationContext);
		chain.setLoaders(loaders);
		chain.afterPropertiesSet();
	}

	public void setLoader(ResourcePatternResolver loader) {
		this.loader = loader;
	}
}