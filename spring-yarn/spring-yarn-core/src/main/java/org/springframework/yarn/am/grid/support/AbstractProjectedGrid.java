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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.springframework.util.Assert;
import org.springframework.yarn.am.grid.Grid;
import org.springframework.yarn.am.grid.GridMember;
import org.springframework.yarn.am.grid.GridProjection;
import org.springframework.yarn.am.grid.ProjectedGrid;
import org.springframework.yarn.am.grid.listener.DefaultProjectedGridListener;
import org.springframework.yarn.am.grid.listener.GridListener;
import org.springframework.yarn.am.grid.listener.ProjectedGridListener;

/**
 * Simple {@code ProjectedGrid} base implementation.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractProjectedGrid implements ProjectedGrid {

	private final Collection<GridProjection> projections = new HashSet<GridProjection>();

	/** Listener dispatcher for projected grid events */
	private final DefaultProjectedGridListener listeners = new DefaultProjectedGridListener();

	private final Grid grid;

	/**
	 * Instantiates a new abstract projected grid.
	 *
	 * @param grid the grid
	 */
	public AbstractProjectedGrid(Grid grid) {
		Assert.notNull(grid, "Grid must not be null");
		this.grid = grid;
		this.grid.addInterceptor(new ProjectionAcceptInterceptor());
		this.grid.addGridListener(new ProjectionHandlingGridListener());
	}

	@Override
	public boolean addProjection(GridProjection projection) {
		Assert.notNull(projection, "Projection must not be null");
		return projections.add(projection);
	}

	@Override
	public boolean removeProjection(GridProjection projection) {
		Assert.notNull(projection, "Projection must not be null");
		boolean removed = projections.remove(projection);
		if (removed) {
			notifyGridProjectionRemoved(projection);
		}
		return removed;
	}

	@Override
	public Collection<GridProjection> getProjections() {
		return projections;
	}

	@Override
	public void addProjectedGridListener(ProjectedGridListener listener) {
		listeners.register(listener);
	}

	/**
	 * Notifies registered {@code ProjectedGridListener}s that
	 * a {@code GridProjection} has been added to a {@code ProjectedGrid}.
	 *
	 * @param projection the grid projection
	 */
	protected void notifyGridProjectionAdded(GridProjection projection) {
		listeners.projectionAdded(projection);
	}

	/**
	 * Notifies registered {@code ProjectedGridListener}s that
	 * a {@code GridProjection} has been removed from a {@code ProjectedGrid}.
	 *
	 * @param projection the grid projection
	 */
	protected void notifyGridProjectionRemoved(GridProjection projection) {
		listeners.projectionRemoved(projection);
	}

	/**
	 * Notifies registered {@code ContainerGridGroupsListener}s that
	 * a {@code ContainerNode} has been added to a {@code ContainerGroup}.
	 *
	 * @param projection the grid projection
	 * @param member the grid member
	 */
	protected void notifyMemberAdded(GridProjection projection, GridMember member) {
		listeners.memberAdded(projection, member);
	}

	/**
	 * Notifies registered {@code ContainerGridGroupsListener}s that
	 * a {@code ContainerNode} has been removed from a {@code ContainerGroup}.
	 *
	 * @param projection the group
	 * @param member the node
	 */
	protected void notifyMemberRemoved(GridProjection projection, GridMember member) {
		listeners.memberRemoved(projection, member);
	}

	private class ProjectionAcceptInterceptor implements GridMemberInterceptor {

		@Override
		public GridMember preAdd(GridMember member, Grid grid) {
			// intercept and check if any projection accepts this member
			// using a priority sorted list to offer members to
			// lowest priorities
			List<GridProjection> projections = new ArrayList<GridProjection>(getProjections());
			Collections.sort(projections, GridProjection.PRIORITY_COMPARATOR);
			for (GridProjection projection : projections) {
				if (projection.acceptMember(member)) {
					notifyMemberAdded(projection, member);
					return member;
				}
			}
			return null;
		}

	}

	private class ProjectionHandlingGridListener implements GridListener {

		@Override
		public void memberAdded(GridMember member) {
		}

		@Override
		public void memberRemoved(GridMember member) {
			for (GridProjection projection : projections) {
				if (projection.removeMember(member) != null) {
					notifyMemberRemoved(projection, member);
				}
			}
		}

	}

}
