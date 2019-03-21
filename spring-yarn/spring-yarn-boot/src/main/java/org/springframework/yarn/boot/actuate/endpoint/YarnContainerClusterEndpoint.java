/*
 * Copyright 2014-2015 the original author or authors.
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
package org.springframework.yarn.boot.actuate.endpoint;

import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.boot.actuate.endpoint.AbstractEndpoint;
import org.springframework.boot.actuate.endpoint.Endpoint;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.yarn.YarnSystemConstants;
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
public class YarnContainerClusterEndpoint extends AbstractEndpoint<Map<String, ContainerCluster>> implements ApplicationContextAware {

	public final static String ENDPOINT_ID = "yarn_containercluster";

	private ApplicationContext applicationContext;

	/**
	 * Instantiates a new yarn container cluster endpoint.
	 */
	public YarnContainerClusterEndpoint() {
		super(ENDPOINT_ID);
	}


	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public Map<String, ContainerCluster> invoke() {
		return getClusters();
	}

	public Map<String, ContainerCluster> getClusters() {
		return getAppmaster().getContainerClusters();
	}

	public ContainerCluster createCluster(String clusterId, String clusterDef, ProjectionData projectionData, Map<String, Object> extraProperties) {
		return getAppmaster().createContainerCluster(clusterId, clusterDef, projectionData, extraProperties);
	}

	public void startCluster(String clusterId) {
		getAppmaster().startContainerCluster(clusterId);
	}

	public void stopCluster(String clusterId) {
		getAppmaster().stopContainerCluster(clusterId);
	}

	public void destroyCluster(String clusterId) {
		getAppmaster().destroyContainerCluster(clusterId);
	}

	public void modifyCluster(String id, ProjectionData projectionData) {
		getAppmaster().modifyContainerCluster(id, projectionData);
	}

	private ContainerClusterAppmaster getAppmaster() {
		YarnAppmaster yarnAppmaster = applicationContext.getBean(YarnAppmaster.class,
				YarnSystemConstants.DEFAULT_ID_APPMASTER);
		if (yarnAppmaster instanceof ContainerClusterAppmaster) {
			return ((ContainerClusterAppmaster)yarnAppmaster);
		}
		throw new InvalidAppmasterException("Appmaster of type ContainerClusterAppmaster not found");
	}

	@SuppressWarnings("serial")
	@ResponseStatus(value = HttpStatus.NOT_FOUND, reason = "Appmaster not found")
	private static class InvalidAppmasterException extends RuntimeException {

		public InvalidAppmasterException(String string) {
			super(string);
		}

	}

}
