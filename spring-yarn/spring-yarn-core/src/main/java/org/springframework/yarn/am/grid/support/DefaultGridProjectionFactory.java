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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.springframework.yarn.am.grid.GridProjection;
import org.springframework.yarn.am.grid.GridProjectionFactory;

/**
 * A default implementation of a {@link GridProjectionFactory}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultGridProjectionFactory implements GridProjectionFactory {

	private final static Set<String> REG_NAMES = new HashSet<String>(Arrays.asList(DefaultGridProjection.REGISTERED_NAME));

	/**
	 * Instantiates a new default grid projection factory.
	 */
	public DefaultGridProjectionFactory() {
	}

	@Override
	public GridProjection getGridProjection(ProjectionData projectionData, Configuration configuration) {
		DefaultGridProjection projection = null;
		if (DefaultGridProjection.REGISTERED_NAME.equalsIgnoreCase(projectionData.getType())) {
			projection = new DefaultGridProjection();
			projection.setConfiguration(configuration);
			projection.setPriority(projectionData.getPriority());
			projection.setMemory(projectionData.getMemory());
			projection.setVirtualCores(projectionData.getVirtualCores());
			if (projectionData.getLocality() != null) {
				projection.setLocality(projectionData.getLocality());
			}
			projection.setProjectionData(projectionData);
		}
		return projection;
	}

	@Override
	public Set<String> getRegisteredProjectionTypes() {
		return REG_NAMES;
	}

}
