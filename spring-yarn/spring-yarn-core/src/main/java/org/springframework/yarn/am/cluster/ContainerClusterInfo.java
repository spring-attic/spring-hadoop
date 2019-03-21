/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.yarn.am.cluster;

import org.springframework.yarn.am.grid.GridProjection;

public class ContainerClusterInfo {

	private String clusterId;
	private GridProjection projection;

	public ContainerClusterInfo(String clusterId, GridProjection projection) {
		this.clusterId = clusterId;
		this.projection = projection;
	}

	public String getClusterId() {
		return clusterId;
	}

	public GridProjection getProjection() {
		return projection;
	}

}
