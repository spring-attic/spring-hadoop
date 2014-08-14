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

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.boot.actuate.endpoint.YarnContainerClusterEndpoint;
import org.springframework.yarn.boot.actuate.endpoint.mvc.AbstractContainerClusterRequest.ProjectionDataType;
import org.springframework.yarn.boot.actuate.endpoint.mvc.ContainerClusterCreateRequest;
import org.springframework.yarn.boot.actuate.endpoint.mvc.ContainerClusterModifyRequest;
import org.springframework.yarn.boot.actuate.endpoint.mvc.ContainerClusterModifyRequest.ModifyAction;
import org.springframework.yarn.boot.actuate.endpoint.mvc.domain.ContainerClusterResource;
import org.springframework.yarn.boot.actuate.endpoint.mvc.domain.YarnContainerClusterEndpointResource;

/**
 * Tests for {@link YarnContainerClusterTemplate} and {@link YarnContainerClusterOperations}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnContainerClusterTemplateTests {

	private MockRestServiceServer mockServer;

	private RestTemplate restTemplate;

	private YarnContainerClusterOperations operations;

	@Before
	public void setUp() {
		restTemplate = new RestTemplate();
		mockServer = MockRestServiceServer.createServer(restTemplate);
		operations = new YarnContainerClusterTemplate("/" + YarnContainerClusterEndpoint.ENDPOINT_ID, restTemplate);
	}

	@Test
	public void testHome() {
		String responseBody = "{\"clusters\":[]}";
		mockServer.
			expect(requestTo("/" + YarnContainerClusterEndpoint.ENDPOINT_ID)).
			andExpect(method(HttpMethod.GET)).
			andRespond(withSuccess(responseBody, MediaType.APPLICATION_JSON));
		YarnContainerClusterEndpointResource response = operations.getClusters();
		assertThat(response, notNullValue());
	}

	@Test
	public void testStart() {
		String responseBody = "{" + "\"id\":\"cluster1\","
				+ "\"gridProjection\":{\"members\":[],\"projectionData\":{\"any\":1,\"hosts\":{},\"racks\":{}},"
				+ "\"satisfyState\":null},\"containerClusterState\":{\"clusterState\":\"RUNNING\"}" + "}";
		mockServer.
			expect(requestTo("/" + YarnContainerClusterEndpoint.ENDPOINT_ID + "/cluster1")).
			andExpect(method(HttpMethod.PUT)).
			andExpect(jsonPath("$.*", hasSize(2))).
			andExpect(jsonPath("$.projectionData", nullValue())).
			andExpect(jsonPath("$.action", is(ModifyAction.START.toString().toLowerCase()))).
			andRespond(withSuccess(responseBody, MediaType.APPLICATION_JSON));
		ContainerClusterModifyRequest request = new ContainerClusterModifyRequest();
		request.setAction("start");
		ContainerClusterResource response = operations.clusterStart("cluster1", request);
		assertThat(response, notNullValue());
	}

	@Test
	public void testStop() {
		String responseBody = "{" + "\"id\":\"cluster1\","
				+ "\"gridProjection\":{\"members\":[],\"projectionData\":{\"any\":1,\"hosts\":{},\"racks\":{}},"
				+ "\"satisfyState\":null},\"containerClusterState\":{\"clusterState\":\"STOPPING\"}" + "}";
		mockServer.
			expect(requestTo("/" + YarnContainerClusterEndpoint.ENDPOINT_ID + "/cluster1")).
			andExpect(method(HttpMethod.PUT)).
			andExpect(jsonPath("$.*", hasSize(2))).
			andExpect(jsonPath("$.projectionData", nullValue())).
			andExpect(jsonPath("$.action", is(ModifyAction.STOP.toString().toLowerCase()))).
			andRespond(withSuccess(responseBody, MediaType.APPLICATION_JSON));
		ContainerClusterModifyRequest request = new ContainerClusterModifyRequest();
		request.setAction("stop");
		ContainerClusterResource response = operations.clusterStop("cluster1", request);
		assertThat(response, notNullValue());
	}

	@Test
	public void testCreateCluster() {
		String responseBody = "{" + "\"id\":\"cluster1\","
				+ "\"gridProjection\":{\"members\":[],\"projectionData\":{\"any\":1,\"hosts\":{},\"racks\":{}},"
				+ "\"satisfyState\":null},\"containerClusterState\":null" + "}";

		mockServer.expect(requestTo("/" + YarnContainerClusterEndpoint.ENDPOINT_ID)).andExpect(method(HttpMethod.POST))
		.andRespond(withSuccess(responseBody, MediaType.APPLICATION_JSON));

		ContainerClusterCreateRequest request = new ContainerClusterCreateRequest();
		request.setClusterId("cluster1");
		request.setProjection("any");
		ProjectionDataType projectionData = new ProjectionDataType();
		projectionData.setAny(1);
		request.setProjectionData(projectionData);

		ContainerClusterResource response = operations.clusterCreate(request);
		assertThat(response, notNullValue());
	}

}
