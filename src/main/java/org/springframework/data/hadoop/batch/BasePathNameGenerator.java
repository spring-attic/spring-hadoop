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

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

/**
 * Generator that removes or adds the base path to/from the head of the given names.
 * 
 * @author Costin Leau
 */
public class BasePathNameGenerator implements InitializingBean, ResourceLoaderAware, NameGenerator {

	private ResourceLoader resourceLoader;
	private String path;
	private boolean remove = true;
	private String basePath;

	public String generate(String original) {
		System.out.println("Received path " + original);
		if (original.startsWith(basePath)) {
			if (remove) {
				return original.substring(basePath.length());
			}
		}
		else {
			if (!remove) {
				return basePath.concat(original);
			}
		}
		return original;
	}

	public void setResourceLoader(ResourceLoader resourceLoader) {
		this.resourceLoader = resourceLoader;
	}

	public void afterPropertiesSet() throws Exception {
		Resource resource = resourceLoader.getResource(path);
		basePath = resource.getURI().toString();
		if (!path.endsWith("/")) {
			basePath = basePath.substring(0, basePath.lastIndexOf("/"));
		}
		System.out.println("Base path is " + basePath);
	}

	/**
	 * @param path The path to set.
	 */
	public void setPath(String path) {
		this.path = path;
	}

	/**
	 * @param remove The remove to set.
	 */
	public void setRemove(boolean remove) {
		this.remove = remove;
	}
}