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

import org.springframework.yarn.am.grid.Grid;
import org.springframework.yarn.am.grid.ProjectedGrid;

/**
 * Default implementation of {@link ProjectedGrid}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultProjectedGrid extends AbstractProjectedGrid {

	/**
	 * Instantiates a new default projected grid.
	 *
	 * @param grid the grid
	 */
	public DefaultProjectedGrid(Grid grid) {
		super(grid);
	}

}
