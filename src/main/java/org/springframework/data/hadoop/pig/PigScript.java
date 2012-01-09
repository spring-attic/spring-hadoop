/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.hadoop.pig;

import java.util.Map;

import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

/**
 * Holder class for a pig script.
 *
 * @author Costin Leau
 */
public class PigScript {

	private Resource resource;
	private Map<String, String> params;

	public PigScript(Resource resource) {
		this(resource, null);
	}

	public PigScript(Resource resource, Map<String, String> params) {
		Assert.notNull(resource, "a valid resource is required");
		this.resource = resource;
		this.params = params;
	}

	/**
	 * Returns the resource.
	 *
	 * @return Returns the resource
	 */
	public Resource getResource() {
		return resource;
	}

	/**
	 * Returns the params.
	 *
	 * @return Returns the params
	 */
	public Map<String, String> getParams() {
		return params;
	}
}
