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
package org.springframework.yarn.boot.actuate.endpoint.mvc;

import static org.springframework.web.servlet.mvc.method.annotation.MvcUriComponentsBuilder.on;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.MvcUriComponentsBuilder;
import org.springframework.web.util.UriComponents;
import org.springframework.yarn.am.cluster.ContainerCluster;
import org.springframework.yarn.am.grid.support.ProjectionData;
import org.springframework.yarn.boot.actuate.endpoint.YarnContainerClusterEndpoint;
import org.springframework.yarn.boot.actuate.endpoint.mvc.ContainerClusterModifyRequest.ModifyAction;
import org.springframework.yarn.boot.actuate.endpoint.mvc.domain.ContainerClusterResource;
import org.springframework.yarn.boot.actuate.endpoint.mvc.domain.YarnContainerClusterEndpointResource;

/**
 * A {@link RestController} adding specific rest API used to
 * control {@link YarnContainerClusterEndpoint}.
 *
 * @author Janne Valkealahti
 *
 */
@RestController
@RequestMapping(YarnContainerClusterEndpoint.ENDPOINT_ID)
public class YarnContainerClusterMvcEndpoint{

	private final YarnContainerClusterEndpoint delegate;

	/**
	 * Instantiates a new yarn container cluster mvc endpoint.
	 *
	 * @param delegate the delegate {@link YarnContainerClusterEndpoint}
	 */
	public YarnContainerClusterMvcEndpoint(YarnContainerClusterEndpoint delegate) {
		this.delegate = delegate;
	}

	/**
	 * Main {@link EndpointMvcAdapter#invoke()} which returns information
	 * about existing container clusters.
	 */
	@GetMapping
	@ResponseBody
	public Object invoke() {
		Collection<ContainerCluster> clusters = delegate.getClusters().values();
		YarnContainerClusterEndpointResource response = new YarnContainerClusterEndpointResource();
		Collection<String> clusterIds = new ArrayList<>();
		for (ContainerCluster cluster : clusters) {
			clusterIds.add(cluster.getId());
		}
		response.setClusters(clusterIds);
		return response;
	}

	/**
	 * Creates a new container cluster.
	 *
	 * @param request the container cluster create request
	 * @return the container cluster create response
	 */
	@PostMapping
	public HttpEntity<Void> createCluster(@RequestBody ContainerClusterCreateRequest request) {
		ProjectionData projectionData = new ProjectionData();
		if (request.getProjectionData().getAny() != null) {
			projectionData.setAny(request.getProjectionData().getAny());
		}
		if (request.getProjectionData().getHosts() != null) {
			projectionData.setHosts(request.getProjectionData().getHosts());
		}
		if (request.getProjectionData().getRacks() != null) {
			projectionData.setRacks(request.getProjectionData().getRacks());
		}
		if (request.getProjectionData().getProperties() != null) {
			projectionData.setProperties(request.getProjectionData().getProperties());
		}

		if (request.getProjection() == null) {
			throw new InvalidInputException("Projection not defined");
		}

		projectionData.setType(request.getProjection().toLowerCase());

		Map<String, Object> extraProperties = request.getExtraProperties();

		delegate.createCluster(request.getClusterId(), request.getClusterDef(), projectionData, extraProperties);

		HttpHeaders responseHeaders = new HttpHeaders();
		UriComponents uriComponents = MvcUriComponentsBuilder
			    .fromMethodCall(on(YarnContainerClusterMvcEndpoint.class).clusterInfo(request.getClusterId())).build();
		responseHeaders.setLocation(uriComponents.toUri());

		return new ResponseEntity<>(responseHeaders, HttpStatus.CREATED);
	}

	/**
	 * Gets a status of a specific container cluster.
	 *
	 * @param clusterId the container cluster identifier
	 * @return the container cluster status response
	 */
	@GetMapping(value = "/{clusterId:.*}")
	public HttpEntity<ContainerClusterResource> clusterInfo(@PathVariable("clusterId") String clusterId) {
		ContainerCluster cluster = delegate.getClusters().get(clusterId);
		if (cluster == null) {
			throw new NoSuchClusterException("No such cluster: " + clusterId);
		}
		ContainerClusterResource response = new ContainerClusterResource(cluster);
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	/**
	 * Modifies a container cluster state.
	 *
	 * @param clusterId the container cluster identifier
	 * @param request Binding for modify request content
	 * @return the container cluster status response
	 */
	@PutMapping(value = "/{clusterId:.*}")
	public HttpEntity<Void> modifyCluster(@PathVariable("clusterId") String clusterId,
			@RequestBody ContainerClusterModifyRequest request) {
		ModifyAction action = ContainerClusterModifyRequest.getModifyAction(request.getAction());
		if (action == null) {
			throw new NoSuchActionException("Action " + request.getAction() + " not supported");
		}
		getClusterMayThrow(clusterId);
		if (ModifyAction.START.equals(action)) {
			delegate.startCluster(clusterId);
		} else if (ModifyAction.STOP.equals(action)) {
			delegate.stopCluster(clusterId);
		}
		return new ResponseEntity<>(HttpStatus.OK);
	}

	@DeleteMapping(value = "/{clusterId:.*}")
	public ResponseEntity<Void> destroyCluster(@PathVariable("clusterId") String clusterId) {
		ContainerCluster cluster = getClusterMayThrow(clusterId);
		delegate.destroyCluster(cluster.getId());
		return new ResponseEntity<>(HttpStatus.OK);
	}

	/**
	 * Modify a container cluster
	 *
	 * @param clusterId the container cluster identifier
	 * @param request the request
	 * @return the container cluster modify response
	 */
	@PatchMapping(value = "/{clusterId:.*}")
	public HttpEntity<Void> updateCluster(@PathVariable("clusterId") String clusterId, @RequestBody ContainerClusterCreateRequest request) {
		ContainerCluster cluster = delegate.getClusters().get(clusterId);
		if (cluster == null) {
			throw new NoSuchClusterException("No such cluster: " + clusterId);
		}
		ProjectionData data = new ProjectionData();
		if (request.getProjectionData().getAny() != null) {
			data.setAny(request.getProjectionData().getAny());
		}
		if (request.getProjectionData().getHosts() != null) {
			data.setHosts(request.getProjectionData().getHosts());
		}
		if (request.getProjectionData().getRacks() != null) {
			data.setRacks(request.getProjectionData().getRacks());
		}
		delegate.modifyCluster(clusterId, data);

		return new ResponseEntity<>(HttpStatus.OK);
	}

	/**
	 * Gets a cluster and automatically throws a {@link NoSuchClusterException}
	 * if cluster doesn't exist.
	 *
	 * @param clusterId the cluster id
	 * @return container cluster if found
	 */
	private ContainerCluster getClusterMayThrow(String clusterId) {
		ContainerCluster cluster = delegate.getClusters().get(clusterId);
		if (cluster == null) {
			throw new NoSuchClusterException("No such cluster: " + clusterId);
		}
		return cluster;
	}

	@SuppressWarnings("serial")
	@ResponseStatus(value = HttpStatus.NOT_FOUND, reason = "No such cluster")
	private static class NoSuchClusterException extends RuntimeException {

		public NoSuchClusterException(String string) {
			super(string);
		}

	}

	@SuppressWarnings("serial")
	@ResponseStatus(value = HttpStatus.NOT_FOUND, reason = "No such action")
	private static class NoSuchActionException extends RuntimeException {

		public NoSuchActionException(String string) {
			super(string);
		}

	}

	@SuppressWarnings("serial")
	@ResponseStatus(value = HttpStatus.METHOD_NOT_ALLOWED, reason = "Invalid input")
	private static class InvalidInputException extends RuntimeException {

		public InvalidInputException(String string) {
			super(string);
		}

	}

}
