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

import java.util.Map;

/**
 *
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerClusterCreateRequest extends AbstractContainerClusterRequest {

	private String clusterId;

	private String clusterDef;

	private String projection;

	private ProjectionDataType projectionData;

	private Map<String, Object> extraProperties;

	public String getClusterId() {
		return clusterId;
	}

	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}

	public String getClusterDef() {
		return clusterDef;
	}

	public void setClusterDef(String clusterDef) {
		this.clusterDef = clusterDef;
	}

	public String getProjection() {
		return projection;
	}

	public void setProjection(String projection) {
		this.projection = projection;
	}

	public ProjectionDataType getProjectionData() {
		return projectionData;
	}

	public void setProjectionData(ProjectionDataType projectionData) {
		this.projectionData = projectionData;
	}

	public Map<String, Object> getExtraProperties() {
		return extraProperties;
	}

	public void setExtraProperties(Map<String, Object> extraProperties) {
		this.extraProperties = extraProperties;
	}

}
