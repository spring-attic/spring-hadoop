/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.yarn.fs;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.LocalResource;

/**
 * Default implementation of a {@link MultiResourceLocalizer}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultMultiResourceLocalizer implements MultiResourceLocalizer {

	private final HashMap<String, ResourceLocalizer> localizers = new HashMap<String, ResourceLocalizer>();

	/**
	 * Instantiates a new default multi resource localizer.
	 *
	 * @param localizer the default localizer
	 * @param localizers the custom localizers
	 */
	public DefaultMultiResourceLocalizer(ResourceLocalizer localizer, Map<String, ResourceLocalizer> localizers) {
		this.localizers.put(null, localizer);
		this.localizers.putAll(localizers);
	}

	@Override
	public Map<String, LocalResource> getResources() {
		return getResources(null);
	}

	@Override
	public Map<String, LocalResource> getResources(String id) {
		ResourceLocalizer localizer = localizers.get(id);
		if (localizer == null) {
			localizer = localizers.get(null);
		}
		if (localizer != null) {
			return localizer.getResources();
		} else {
			return null;
		}
	}

}
