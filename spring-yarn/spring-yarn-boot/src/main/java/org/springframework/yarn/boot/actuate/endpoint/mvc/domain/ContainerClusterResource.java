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

import org.springframework.yarn.am.cluster.ClusterState;
import org.springframework.yarn.am.cluster.ContainerCluster;

/**
 * Domain mapping for {@link ContainerCluster}.
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerClusterResource {

	private String id;

	private GridProjectionResource gridProjection;

	private ContainerClusterStateResource containerClusterState;

	public ContainerClusterResource() {
	}

	public ContainerClusterResource(ContainerCluster cluster) {
		this.id = cluster.getId();
		this.gridProjection = new GridProjectionResource(cluster.getGridProjection());
		ClusterState clusterState = cluster.getStateMachine().getState().getId();
		containerClusterState = new ContainerClusterStateResource(clusterState);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public GridProjectionResource getGridProjection() {
		return gridProjection;
	}

	public ContainerClusterStateResource getContainerClusterState() {
		return containerClusterState;
	}

	public void setContainerClusterState(ContainerClusterStateResource containerClusterState) {
		this.containerClusterState = containerClusterState;
	}

}
