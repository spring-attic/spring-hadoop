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
package org.springframework.yarn.boot.actuate.endpoint.mvc.domain;

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.yarn.am.grid.GridMember;
import org.springframework.yarn.am.grid.GridProjection;
import org.springframework.yarn.am.grid.support.ProjectionData;

public class GridProjectionResource {

	private Collection<GridMemberResource> members;

	private ProjectionData projectionData;

	private SatisfyStateDataResource satisfyState;

	public GridProjectionResource() {
	}

	public GridProjectionResource(GridProjection projection) {
		this.members = new ArrayList<GridMemberResource>();
		for (GridMember member : projection.getMembers()) {
			this.members.add(new GridMemberResource(member.getId().toString()));
		}
		projectionData = projection.getProjectionData();
		satisfyState = new SatisfyStateDataResource(projection.getSatisfyState());
	}

	public Collection<GridMemberResource> getMembers() {
		return members;
	}

	public void setMembers(Collection<GridMemberResource> members) {
		this.members = members;
	}

	public ProjectionData getProjectionData() {
		return projectionData;
	}

	public SatisfyStateDataResource getSatisfyState() {
		return satisfyState;
	}

	public void setSatisfyState(SatisfyStateDataResource satisfyState) {
		this.satisfyState = satisfyState;
	}

}
