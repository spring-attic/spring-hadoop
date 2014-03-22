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

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.springframework.yarn.test.context.YarnCluster;

/**
 * Standalone simple mini cluster having Yarn
 * and Hdfs nodes.
 *
 * @author Janne Valkealahti
 *
 */
public class StandaloneYarnCluster implements YarnCluster {

	private final static Log log = LogFactory.getLog(StandaloneYarnCluster.class);

	/** Yarn specific mini cluster */
	private MiniYARNCluster yarnCluster = null;

	/** Hdfs specific mini cluster */
	private MiniDFSCluster dfsCluster = null;

	/** Unique cluster name */
	private final String clusterName;

	/** Configuration build at runtime */
	private Configuration configuration;

	/** Monitor sync for start and stop */
	private final Object startupShutdownMonitor = new Object();

	/** Flag for cluster state */
	private boolean started;

	/** Number of nodes for yarn and dfs */
	private int nodes = 1;

	/**
	 * Instantiates a mini cluster with default
	 * cluster node count.
	 *
	 * @param clusterName the unique cluster name
	 */
	public StandaloneYarnCluster(String clusterName) {
		this.clusterName = clusterName;
	}

	/**
	 * Instantiates a mini cluster with given
	 * cluster node count.
	 *
	 * @param clusterName the unique cluster name
	 * @param nodes the node count
	 */
	public StandaloneYarnCluster(String clusterName, int nodes) {
		this.clusterName = clusterName;
		this.nodes = nodes;
	}

	@Override
	public Configuration getConfiguration() {
		return configuration;
	}

	@Override
	public void start() throws IOException {
		log.info("Checking if cluster=" + clusterName + " needs to be started");
		synchronized (this.startupShutdownMonitor) {
			if (started) {
				return;
			}

			// SHDP-309 hack to set hadoop.security.token.service.use_ip
			// for what we want because 2.3.0 added core-site.xml in
			// jar container MiniYARNCluster which sets this value to false
			// while default is true.
			// we try to add new default resource which should override
			// one from a core-site.xml
			Configuration.addDefaultResource("shdp-site.xml");

			log.info("Starting cluster=" + clusterName);
			configuration = new YarnConfiguration();
			configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, "target/" + clusterName + "-dfs");

			dfsCluster = new MiniDFSCluster.Builder(configuration).
					numDataNodes(nodes).
					build();

			yarnCluster = new MiniYARNCluster(clusterName, nodes, 1, 1);
			yarnCluster.init(configuration);
			yarnCluster.start();

			log.info("Started cluster=" + clusterName);
			started = true;
		}
	}

	@Override
	public void stop() {
		log.info("Checking if cluster=" + clusterName + " needs to be stopped");
		synchronized (this.startupShutdownMonitor) {
			if (!started) {
				return;
			}
			if (yarnCluster != null) {
				yarnCluster.stop();
				yarnCluster = null;
			}
			if (dfsCluster != null) {
				dfsCluster.shutdown();
				dfsCluster = null;
			}
			log.info("Stopped cluster=" + clusterName);
			started = false;
		}
	}

	@Override
	public File getYarnWorkDir() {
		return yarnCluster != null ? yarnCluster.getTestWorkDir() : null;
	}

	/**
	 * Sets a number of nodes for cluster. Every node
	 * will act as yarn and dfs role. Default is one node.
	 *
	 * @param nodes the number of nodes
	 */
	public void setNodes(int nodes) {
		this.nodes = nodes;
	}

}
