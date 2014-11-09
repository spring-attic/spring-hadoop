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
package org.springframework.yarn.am.grid.support;

import java.util.HashMap;
import java.util.Map;

import org.springframework.yarn.am.grid.GridProjection;

/**
 * Helper class to keep info about default projection datas
 * defined in a configuration.
 *
 * @author Janne Valkealahti
 *
 */
public class ProjectionDataRegistry {

	private final Map<String, ProjectionData> defaults = new HashMap<String, ProjectionData>();

	/**
	 * Instantiates a new projection data registry.
	 *
	 * @param defaults the defaults for name to projection data mappings
	 */
	public ProjectionDataRegistry(Map<String, ProjectionData> defaults) {
		if (defaults != null) {
			this.defaults.putAll(defaults);
		}
	}

	/**
	 * Gets all projection datas mapped with an identifier known
	 * to this registry. User of this factory can then use this
	 * information to create a default {@link GridProjection}s
	 * at startup.
	 *
	 * @return the mapping from identifier to projection data
	 */
	public Map<String, ProjectionData> getProjectionDatas() {
		return defaults;
	}

}
