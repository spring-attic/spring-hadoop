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
package org.springframework.yarn.am.grid;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.springframework.yarn.am.grid.support.ProjectionData;

/**
 * A {@link GridProjectionFactory} is used from an application master
 * responsible handling creation of {@link GridProjection}s.
 *
 * @author Janne Valkealahti
 *
 */
public interface GridProjectionFactory {

	/**
	 * Builds a {@link GridProjection} using a {@link ProjectionData}.
	 *
	 * @param projectionData the projection data
	 * @param configuration the hadoop configuration
	 * @return the grid projection
	 */
	GridProjection getGridProjection(ProjectionData projectionData, Configuration configuration);

	/**
	 * Gets a registered projection types handled by this factory.
	 *
	 * @return the set of projection types
	 */
	Set<String> getRegisteredProjectionTypes();

}
