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
package org.springframework.yarn.am.grid.listener;

import org.springframework.yarn.am.grid.GridMember;
import org.springframework.yarn.am.grid.GridProjection;

/**
 * Listener for grid container group events.
 *
 * @author Janne Valkealahti
 *
 */
public interface ProjectedGridListener {

	/**
	 * Invoked when a new projection is added.
	 *
	 * @param projection the {@link GridProjection}
	 */
	void projectionAdded(GridProjection projection);

	/**
	 * Invoked when projection is removed.
	 *
	 * @param projection the {@link GridProjection}
	 */
	void projectionRemoved(GridProjection projection);

	/**
	 * Invoked when a member is added into a projection.
	 *
	 * @param projection the {@link GridProjection} a member belongs to
	 * @param member the {@link GridMember}
	 */
	void memberAdded(GridProjection projection, GridMember member);

	/**
	 * Invoked when a member is removed from a projection.
	 *
	 * @param projection the {@link GridProjection} a member belongs to
	 * @param member the {@link GridMember}
	 */
	void memberRemoved(GridProjection projection, GridMember member);

}
