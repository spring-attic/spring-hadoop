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
package org.springframework.yarn.boot.actuate.endpoint;

import java.util.Map;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.AbstractEndpoint;
import org.springframework.boot.actuate.endpoint.Endpoint;
import org.springframework.util.Assert;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.cluster.ContainerCluster;
import org.springframework.yarn.am.cluster.ContainerClusterAppmaster;
import org.springframework.yarn.am.grid.support.ProjectionData;

/**
 * {@link Endpoint} handling operations against {@link ContainerClusterAppmaster}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnContainerClusterEndpoint extends AbstractEndpoint<Map<String, ContainerCluster>> implements InitializingBean {

	public final static String ENDPOINT_ID = "yarn_containercluster";

	private ContainerClusterAppmaster appmaster;

	/**
	 * Instantiates a new yarn container cluster endpoint.
	 */
	public YarnContainerClusterEndpoint() {
		super(ENDPOINT_ID);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(appmaster, "ContainerClusterAppmaster must be set");
	}

	@Override
	public Map<String, ContainerCluster> invoke() {
		return getClusters();
	}

	/**
	 * Sets {@link ContainerClusterAppmaster} if passed {@link YarnAppmaster} is
	 * instance of it.
	 *
	 * @param yarnAppmaster the yarn application master
	 */
	@Autowired
	public void setYarnAppmaster(YarnAppmaster yarnAppmaster) {
		if (yarnAppmaster instanceof ContainerClusterAppmaster) {
			appmaster = ((ContainerClusterAppmaster)yarnAppmaster);
		}
	}

	public Map<String, ContainerCluster> getClusters() {
		return appmaster.getContainerClusters();
	}

	public ContainerCluster createCluster(String clusterId, String clusterDef, ProjectionData projectionData, Map<String, Object> extraProperties) {
		return appmaster.createContainerCluster(clusterId, clusterDef, projectionData, extraProperties);
	}

	public void startCluster(String clusterId) {
		appmaster.startContainerCluster(clusterId);
	}

	public void stopCluster(String clusterId) {
		appmaster.stopContainerCluster(clusterId);
	}

	public void destroyCluster(String clusterId) {
		appmaster.destroyContainerCluster(clusterId);
	}

	public void modifyCluster(String id, ProjectionData projectionData) {
		appmaster.modifyContainerCluster(id, projectionData);
	}

}
