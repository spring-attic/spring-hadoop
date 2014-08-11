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

import java.util.HashMap;
import java.util.Map;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.boot.actuate.endpoint.mvc.ContainerClusterCreateRequest;
import org.springframework.yarn.boot.actuate.endpoint.mvc.ContainerClusterModifyRequest;
import org.springframework.yarn.boot.actuate.endpoint.mvc.domain.ContainerClusterResource;
import org.springframework.yarn.boot.actuate.endpoint.mvc.domain.YarnContainerClusterEndpointResource;

/**
 * Template accessing boot mvc endpoint controlling container clusters.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnContainerClusterTemplate implements YarnContainerClusterOperations {

	protected final RestTemplate restTemplate;

	private final String baseUri;

	/**
	 * Constructs a {@link YarnContainerClusterTemplate} using a {@link RestTemplate} instantiated
	 * with {@link HttpComponentsClientHttpRequestFactory}.
	 *
	 * @param baseUri the base uri
	 */
	public YarnContainerClusterTemplate(String baseUri) {
		this.restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());
		this.baseUri = baseUri;
	}

	/**
	 * Constructs a {@link YarnContainerClusterTemplate} using a given {@link RestTemplate}.
	 *
	 * @param baseUri the base uri
	 * @param restTemplate the rest template
	 */
	public YarnContainerClusterTemplate(String baseUri, RestTemplate restTemplate) {
		this.restTemplate = restTemplate;
		this.baseUri = baseUri;
	}

	@Override
	public YarnContainerClusterEndpointResource getClusters() throws YarnContainerClusterClientException {
		return restTemplate.getForObject(baseUri, YarnContainerClusterEndpointResource.class);
	}

	@Override
	public ContainerClusterResource clusterCreate(ContainerClusterCreateRequest request) throws YarnContainerClusterClientException {
		try {
			return restTemplate.postForObject(baseUri, request, ContainerClusterResource.class);
		} catch (RestClientException e) {
			throw convertException(e);
		}
	}

	@Override
	public ContainerClusterResource clusterInfo(String clusterId) throws YarnContainerClusterClientException {
		try {
			return restTemplate.getForObject(baseUri + "/" + clusterId, ContainerClusterResource.class);
		} catch (RestClientException e) {
			throw convertException(e);
		}
	}

	@Override
	public ContainerClusterResource clusterStart(String clusterId, ContainerClusterModifyRequest request) throws YarnContainerClusterClientException {
		try {
			HttpEntity<ContainerClusterModifyRequest> requestEntity = new HttpEntity<ContainerClusterModifyRequest>(request);
			ResponseEntity<ContainerClusterResource> exchange = restTemplate.exchange(baseUri + "/" + clusterId,
					HttpMethod.PUT, requestEntity, ContainerClusterResource.class);
			return exchange.getBody();
		} catch (RestClientException e) {
			throw convertException(e);
		}
	}

	@Override
	public ContainerClusterResource clusterStop(String clusterId, ContainerClusterModifyRequest request) throws YarnContainerClusterClientException {
		try {
			HttpEntity<ContainerClusterModifyRequest> requestEntity = new HttpEntity<ContainerClusterModifyRequest>(request);
			ResponseEntity<ContainerClusterResource> exchange = restTemplate.exchange(baseUri + "/" + clusterId,
					HttpMethod.PUT, requestEntity, ContainerClusterResource.class);
			return exchange.getBody();
		} catch (RestClientException e) {
			throw convertException(e);
		}
	}

	@Override
	public ContainerClusterResource clusterModify(String clusterId, ContainerClusterCreateRequest request) throws YarnContainerClusterClientException {
		try {
			HttpEntity<ContainerClusterCreateRequest> requestEntity = new HttpEntity<ContainerClusterCreateRequest>(request);
			ResponseEntity<ContainerClusterResource> exchange = restTemplate.exchange(baseUri + "/" + clusterId,
					HttpMethod.PATCH, requestEntity, ContainerClusterResource.class);
			return exchange.getBody();
		} catch (RestClientException e) {
			throw convertException(e);
		}
	}

	@Override
	public void clusterDestroy(String clusterId) throws YarnContainerClusterClientException {
		try {
			Map<String, String> uriVariables = new HashMap<String, String>();
			uriVariables.put("clusterId", clusterId);
			restTemplate.delete(baseUri + "/{clusterId}", uriVariables);
		} catch (RestClientException e) {
			throw convertException(e);
		}
	}

	private YarnContainerClusterClientException convertException(Exception e) {
		return new YarnContainerClusterClientException("Error communicating with rest endpoint",e);
	}

}
