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

import java.util.Iterator;

import org.springframework.yarn.am.grid.GridMember;
import org.springframework.yarn.am.grid.GridProjection;
import org.springframework.yarn.listener.AbstractCompositeListener;

/**
 * Composite listener for handling container group events.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultProjectedGridListener extends AbstractCompositeListener<ProjectedGridListener>
		implements ProjectedGridListener {

	@Override
	public void projectionAdded(GridProjection group) {
		for (Iterator<ProjectedGridListener> iterator = getListeners().reverse(); iterator.hasNext();) {
			iterator.next().projectionAdded(group);
		}
	}

	@Override
	public void projectionRemoved(GridProjection group) {
		for (Iterator<ProjectedGridListener> iterator = getListeners().reverse(); iterator.hasNext();) {
			iterator.next().projectionRemoved(group);
		}
	}

	@Override
	public void memberAdded(GridProjection group, GridMember node) {
		for (Iterator<ProjectedGridListener> iterator = getListeners().reverse(); iterator.hasNext();) {
			iterator.next().memberAdded(group, node);
		}
	}

	@Override
	public void memberRemoved(GridProjection group, GridMember node) {
		for (Iterator<ProjectedGridListener> iterator = getListeners().reverse(); iterator.hasNext();) {
			iterator.next().memberRemoved(group, node);
		}
	}

}
