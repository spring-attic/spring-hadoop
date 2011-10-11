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
package org.springframework.data.hadoop.batch;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

/**
 * Basic item writer relying on {@link WritableResource}. Uses the provided {@link ResourceLoader}
 * to resolve the resulting resources based on the given {@link NameGenerator}. 
 * If no generator is specified, the resource URI will be used instead for resolving the writable resource.  
 * 
 * Copies the given resources to the generated ones.
 * 
 * @author Costin Leau
 */
public class ResourcesItemWriter implements InitializingBean, ItemWriter<Resource>, ResourceLoaderAware {

	private ResourceLoader resourceLoader;

	private NameGenerator generator;

	public void afterPropertiesSet() {
		Assert.notNull(resourceLoader, "a resource loader is required");
	}

	public void setResourceLoader(ResourceLoader resourceLoader) {
		this.resourceLoader = resourceLoader;
	}

	public void setGenerator(NameGenerator generator) {
		this.generator = generator;
	}

	public void write(List<? extends Resource> items) throws Exception {
		for (Resource resource : items) {
			String uri = resource.getURI().toString();
			String newUri = (generator != null ? generator.generate(uri) : uri);

			Resource out = resourceLoader.getResource(newUri);
			Assert.isTrue(out instanceof WritableResource, "Cannot resolve a writable resource for " + newUri);
			WritableResource wOut = (WritableResource) out;
			Assert.isTrue(wOut.isWritable(), "Writable resources [" + wOut + "] is read-only");
			if (!out.equals(resource)) {
				FileCopyUtils.copy(resource.getInputStream(), wOut.getOutputStream());
			}
		}
	}
}