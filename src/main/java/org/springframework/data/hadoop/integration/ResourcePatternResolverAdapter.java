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
package org.springframework.data.hadoop.integration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.Assert;

public class ResourcePatternResolverAdapter implements InitializingBean, ApplicationContextAware {

	private String locationPattern;

	private Set<Resource> seenResources = Collections.emptySet();

	private ResourcePatternResolver resourcePatternResolver;

	private ResourceLoader ctxLoader;

	public ResourcePatternResolverAdapter() {
	}

	public ResourcePatternResolverAdapter(ResourcePatternResolver resourcePatternResolver, String locationPattern) {
		this.resourcePatternResolver = resourcePatternResolver;
		this.locationPattern = locationPattern;
	}

	public void setLocationPattern(String locationPattern) {
		this.locationPattern = locationPattern;
	}

	public Collection<Resource> getResources() throws IOException {
		Resource[] resources = this.resourcePatternResolver.getResources(locationPattern);
		Set<Resource> set = new LinkedHashSet<Resource>(resources.length);
		List<Resource> newResources = new ArrayList<Resource>();

		for (Resource resource : resources) {
			set.add(resource);
			if (!seenResources.contains(resource)) {
				newResources.add(resource);
			}
		}
		seenResources = set;
		return newResources;
	}

	public void afterPropertiesSet() throws Exception {
		if (resourcePatternResolver == null) {
			if (ctxLoader instanceof ResourcePatternResolver) {
				resourcePatternResolver = (ResourcePatternResolver) ctxLoader;
			}
		}
		Assert.notNull(resourcePatternResolver, "no resource loader specified");
		Assert.hasText(this.locationPattern, "a resource pattern must be specified");
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.ctxLoader = applicationContext;
	}

	/**
	 * @param resourcePatternResolver The resourcePatternResolver to set.
	 */
	public void setResourceLoader(ResourcePatternResolver resourcePatternResolver) {
		this.resourcePatternResolver = resourcePatternResolver;
	}
}