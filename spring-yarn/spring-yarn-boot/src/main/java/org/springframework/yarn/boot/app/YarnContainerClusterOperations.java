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
package org.springframework.yarn.boot.app;

import org.springframework.yarn.boot.actuate.endpoint.mvc.ContainerClusterCreateRequest;
import org.springframework.yarn.boot.actuate.endpoint.mvc.ContainerClusterModifyRequest;
import org.springframework.yarn.boot.actuate.endpoint.mvc.domain.ContainerClusterResource;
import org.springframework.yarn.boot.actuate.endpoint.mvc.domain.YarnContainerClusterEndpointResource;

/**
 * An operations interface for {@link YarnContainerClusterTemplate}.
 *
 * @author Janne Valkealahti
 *
 */
public interface YarnContainerClusterOperations {

	/**
	 * Get a clusters info.
	 *
	 * @return the {@link YarnContainerClusterEndpointResource}
	 * @throws YarnContainerClusterClientException if client experienced an error
	 */
	YarnContainerClusterEndpointResource getClusters() throws YarnContainerClusterClientException;

	/**
	 * Create a container cluster.
	 *
	 * @param request the create request
	 * @return the {@link ContainerClusterResource}
	 * @throws YarnContainerClusterClientException if client experienced an error
	 */
	ContainerClusterResource clusterCreate(ContainerClusterCreateRequest request)
			throws YarnContainerClusterClientException;

	/**
	 * Get a container cluster info.
	 *
	 * @param clusterId the cluster identifier
	 * @return the {@link ContainerClusterResource}
	 * @throws YarnContainerClusterClientException if client experienced an error
	 */
	ContainerClusterResource clusterInfo(String clusterId) throws YarnContainerClusterClientException;

	/**
	 * Start a container cluster.
	 *
	 * @param clusterId the cluster identifier
	 * @param request the modify request.
	 * @return the {@link ContainerClusterResource}
	 * @throws YarnContainerClusterClientException if client experienced an error
	 */
	ContainerClusterResource clusterStart(String clusterId, ContainerClusterModifyRequest request)
			throws YarnContainerClusterClientException;

	/**
	 * Stop a container cluster.
	 *
	 * @param clusterId the cluster identifier
	 * @param request the modify request.
	 * @return the {@link ContainerClusterResource}
	 * @throws YarnContainerClusterClientException if client experienced an error
	 */
	ContainerClusterResource clusterStop(String clusterId, ContainerClusterModifyRequest request)
			throws YarnContainerClusterClientException;

	/**
	 * Modify a container cluster.
	 *
	 * @param clusterId the cluster identifier
	 * @param request the create request.
	 * @return the {@link ContainerClusterResource}
	 * @throws YarnContainerClusterClientException if client experienced an error
	 */
	ContainerClusterResource clusterModify(String clusterId, ContainerClusterCreateRequest request)
			throws YarnContainerClusterClientException;

	/**
	 * Destroy a container cluster.
	 *
	 * @param clusterId the cluster identifier
	 * @throws YarnContainerClusterClientException if client experienced an error
	 */
	void clusterDestroy(String clusterId) throws YarnContainerClusterClientException;

}
