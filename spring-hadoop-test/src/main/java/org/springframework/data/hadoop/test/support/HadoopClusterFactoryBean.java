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
import org.springframework.data.hadoop.test.context.HadoopCluster;
import org.springframework.util.Assert;

/**
 * Factory bean building Hadoop mini clusters.
 * 
 * @author Janne Valkealahti
 *
 */
public class HadoopClusterFactoryBean implements InitializingBean, DisposableBean, FactoryBean<HadoopCluster> {

	/** Instance returned from this factory */
	private StandaloneHadoopCluster cluster;
	
	/** Unique running cluster id */
	private String clusterId;
	
	/** Flag starting cluster from this factory */
	private boolean autoStart;

	/** Number of nodes  */
	private int nodes = 1;
	
	@Override
	public StandaloneHadoopCluster getObject() throws Exception {
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
		Assert.notNull(clusterId, "Cluster id must be set");
		cluster = new StandaloneHadoopCluster(clusterId, nodes);
		if (autoStart) {
			cluster.start();			
		}
	}

	@Override
	public void destroy() throws Exception {
		if (cluster != null) {
			cluster.stop();
		}
	}

	/**
	 * Sets the cluster id. Id must be
	 * unique within running clusters.
	 * 
	 * @param clusterId the cluster id
	 */
	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}
	
	/**
	 * Set whether cluster should be started automatically
	 * by this factory instance. Default setting is false.
	 * 
	 * @param autoStart the flag if cluster should be started automatically
	 */
	public void setAutoStart(boolean autoStart) {
		this.autoStart = autoStart;
	}
	
	/**
	 * Sets the number of nodes.
	 * 
	 * @param nodes the number of nodes
	 */
	public void setNodes(int nodes) {
		this.nodes = nodes;
	}

}
