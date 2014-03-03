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
package org.springframework.data.hadoop.test.support;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.hadoop.test.HadoopTestSystemConstants;
import org.springframework.data.hadoop.test.context.HadoopCluster;

/**
 * Delegates to {@link HadoopClusterManager} for handling lifecycle
 * of {@link HadoopCluster}. This is needed order to clear and shutdown
 * cluster during the application context refresh if
 * {@link org.springframework.test.annotation.DirtiesContext} is used.
 *
 * @author Janne Valkealahti
 *
 */
public class HadoopClusterDelegatingFactoryBean implements InitializingBean, DisposableBean, FactoryBean<HadoopCluster> {

	private HadoopCluster cluster;
	private String id = HadoopTestSystemConstants.DEFAULT_ID_CLUSTER;
	private int nodes = 1;

	@Override
	public HadoopCluster getObject() throws Exception {
		return cluster;
	}

	@Override
	public Class<HadoopCluster> getObjectType() {
		return HadoopCluster.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		cluster = HadoopClusterManager.getInstance(true).getCluster(new ClusterInfo(id, nodes));
	}

	@Override
	public void destroy() throws Exception {
		if (cluster != null) {
			HadoopClusterManager.getInstance().closeCluster(cluster);
		}
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setNodes(int nodes) {
		this.nodes = nodes;
	}

}
