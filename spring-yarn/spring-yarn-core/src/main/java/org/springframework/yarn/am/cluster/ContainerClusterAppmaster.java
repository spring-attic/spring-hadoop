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
package org.springframework.yarn.am.cluster;

import java.util.Map;

import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.grid.support.ProjectionData;

/**
 * Interface for {@link YarnAppmaster} which is controlling
 * container clusters.
 *
 * @author Janne Valkealahti
 *
 */
public interface ContainerClusterAppmaster extends YarnAppmaster {

	Map<String, ContainerCluster> getContainerClusters();

	ContainerCluster createContainerCluster(String clusterId, ProjectionData projection);

	ContainerCluster createContainerCluster(String clusterId, String clusterDef, ProjectionData projection, Map<String, Object> extraProperties);

	void startContainerCluster(String id);

	void stopContainerCluster(String id);

	void destroyContainerCluster(String id);

	void modifyContainerCluster(String id, ProjectionData data);

}
