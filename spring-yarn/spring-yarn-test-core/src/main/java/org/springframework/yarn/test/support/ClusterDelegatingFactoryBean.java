/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.yarn.test.support;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.yarn.test.YarnTestSystemConstants;
import org.springframework.yarn.test.context.YarnCluster;

/**
 * Delegates to {@link YarnClusterManager} for handling lifecycle
 * of {@link YarnCluster}. This is needed order to clear and shutdown
 * cluster during the application context refresh if
 * {@link org.springframework.test.annotation.DirtiesContext} is used.
 * 
 * @author Janne Valkealahti
 *
 */
public class ClusterDelegatingFactoryBean implements InitializingBean, DisposableBean, FactoryBean<YarnCluster> {

	private YarnCluster cluster;
	private String id = YarnTestSystemConstants.DEFAULT_ID_CLUSTER;
	private int nodes = 1;
	
	@Override
	public YarnCluster getObject() throws Exception {
		return cluster;
	}

	@Override
	public Class<YarnCluster> getObjectType() {
		return YarnCluster.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		cluster = YarnClusterManager.getInstance(true).getCluster(new ClusterInfo(id, nodes, nodes));		
	}

	@Override
	public void destroy() throws Exception {
		if (cluster != null) {
			YarnClusterManager.getInstance().closeCluster(cluster);
		}
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public void setNodes(int nodes) {
		this.nodes = nodes;
	}

}
