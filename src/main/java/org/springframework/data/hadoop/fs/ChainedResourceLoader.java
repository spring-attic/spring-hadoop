/*
 * Copyright 2011 the original author or authors.
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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.Assert;

/**
 * Utility class used for chaining multiple {@link ResourceLoader} based on a given prefix.
 * The chained resource loader can then be registered with the application context, allowing
 * only one instance to be injected but delegating the loading per given prefix.
 * 
 * @author Costin Leau
 */
class ChainedResourceLoader implements ApplicationContextAware, InitializingBean, ResourcePatternResolver,
		BeanFactoryPostProcessor, Ordered {

	private ResourcePatternResolver fallback;
	private Map<String, ResourceLoader> resourceLoaders = new ConcurrentHashMap<String, ResourceLoader>(4);
	private Map<String, ResourcePatternResolver> patternLoaders = new ConcurrentHashMap<String, ResourcePatternResolver>(4);
	private Map<String, ? extends ResourceLoader> loaders = Collections.emptyMap();

	public void setLoaders(Map<String, ? extends ResourceLoader> loaders) {
		Assert.notNull(loaders, "a valid map of loaders required");
		this.loaders = loaders;
	}

	public void afterPropertiesSet() {
		Assert.notNull(fallback, "a default resource loader is required");

		for (Map.Entry<String, ? extends ResourceLoader> entry : loaders.entrySet()) {
			ResourceLoader loader = entry.getValue();
			resourceLoaders.put(entry.getKey(), loader);
			if (loader instanceof ResourcePatternResolver) {
				patternLoaders.put(entry.getKey(), (ResourcePatternResolver) loader);
			}
		}
	}

	public Resource[] getResources(String locationPattern) throws IOException {
		ResourcePatternResolver pResolver = resolvePatternResolver(locationPattern);
		return pResolver.getResources(locationPattern);
	}

	public Resource getResource(String location) {
		ResourceLoader resourceLoader = resolveResourceLoader(location);
		return resourceLoader.getResource(location);
	}

	public ClassLoader getClassLoader() {
		return fallback.getClassLoader();
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		if (fallback == null) {
			fallback = applicationContext;
		}

		if (applicationContext instanceof GenericApplicationContext) {
			((GenericApplicationContext) applicationContext).setResourceLoader(this);
		}
	}

	private String getPrefix(String location) {
		int indexOf = location.indexOf("://");
		return (indexOf > 0 ? location.substring(indexOf) : null);
	}

	private ResourcePatternResolver resolvePatternResolver(String locationPattern) {
		String prefix = getPrefix(locationPattern);
		return (prefix != null ? patternLoaders.get(prefix) : fallback);
	}

	private ResourceLoader resolveResourceLoader(String location) {
		String prefix = getPrefix(location);
		ResourceLoader rl = (prefix != null ? resourceLoaders.get(prefix) : fallback);
		return (rl == null ? fallback : rl);
	}

	public int getOrder() {
		return PriorityOrdered.LOWEST_PRECEDENCE;
	}

	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
		Assert.notNull(fallback);
		// no-op - simply added to be sure we're triggered early on
	}

	public void setFallback(ResourcePatternResolver fallback) {
		this.fallback = fallback;
	}
}