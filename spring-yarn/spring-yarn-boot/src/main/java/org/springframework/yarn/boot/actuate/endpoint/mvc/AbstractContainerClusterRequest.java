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
 * Base class for requests handling clusters.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractContainerClusterRequest {

	private ProjectionDataType projectionData;

	public ProjectionDataType getProjectionData() {
		return projectionData;
	}

	public void setProjectionData(ProjectionDataType projectionData) {
		this.projectionData = projectionData;
	}

	public static class ProjectionDataType {
		Integer any;
		Map<String, Integer> hosts;
		Map<String, Integer> racks;
		Map<String, Object> properties;
		public Integer getAny() {
			return any;
		}
		public void setAny(Integer any) {
			this.any = any;
		}
		public Map<String, Integer> getHosts() {
			return hosts;
		}
		public void setHosts(Map<String, Integer> hosts) {
			this.hosts = hosts;
		}
		public Map<String, Integer> getRacks() {
			return racks;
		}
		public void setRacks(Map<String, Integer> racks) {
			this.racks = racks;
		}
		public Map<String, Object> getProperties() {
			return properties;
		}
		public void setProperties(Map<String, Object> properties) {
			this.properties = properties;
		}
	}

}
