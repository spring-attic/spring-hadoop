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

import java.util.Hashtable;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.yarn.test.context.YarnCluster;

/**
 * Manager handling running mini clusters for tests.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnClusterManager {

	private final static Log log = LogFactory.getLog(YarnClusterManager.class);

	/** Singleton instance */
	private static volatile YarnClusterManager instance = null;

	/** Synchronization monitor for the "refresh" and "destroy" */
	private final Object startupShutdownMonitor = new Object();

	/** Reference to the JVM shutdown hook, if registered */
	private Thread shutdownHook;

	/** Cached clusters*/
	private Hashtable<ClusterInfo, YarnCluster> clusters = new Hashtable<ClusterInfo, YarnCluster>();

	/**
	 * Private initializer for singleton access.
	 */
	private YarnClusterManager() {}

	/**
	 * Gets the singleton instance of {@link YarnClusterManager}.
	 *
	 * @return the singleton instance
	 */
	public static YarnClusterManager getInstance() {
		if (instance == null) {
			synchronized (YarnClusterManager.class) {
				if (instance == null) {
					instance = new YarnClusterManager();
				}
			}
		}
		return instance;
	}

	/**
	 * Gets the singleton instance of {@link YarnClusterManager}.
	 *
	 * @param registerShutdownHook if true register shutdown hook
	 * @return the singleton instance
	 */
	public static YarnClusterManager getInstance(boolean registerShutdownHook) {
		YarnClusterManager instance2 = getInstance();
		if (registerShutdownHook) {
			instance2.registerShutdownHook();
		}
		return instance2;
	}

	/**
	 * Gets and starts the mini cluster.
	 *
	 * @param clusterInfo the info about the cluster
	 * @return the running mini cluster
	 */
	public YarnCluster getCluster(ClusterInfo clusterInfo) {
		YarnCluster cluster = clusters.get(clusterInfo);
		if (cluster == null) {
			log.info("Building new cluster for ClusterInfo=" + clusterInfo);
			try {
				YarnClusterFactoryBean fb = new YarnClusterFactoryBean();
				fb.setClusterId("yarn-" + clusterInfo.hashCode());
				fb.setNodes(clusterInfo.getNumYarn());
				fb.setAutoStart(true);
				fb.afterPropertiesSet();
				cluster = fb.getObject();
				clusters.put(clusterInfo, cluster);
			} catch (Exception e) {
			}
		} else {
			log.info("Found cached cluster for ClusterInfo=" + clusterInfo);
		}
		return cluster;
	}

	/**
	 * Closes and remove {@link YarnCluster} from
	 * a manager cache.
	 *
	 * @param cluster the Yarn cluster
	 * @return true if cluster was closed, false otherwise
	 */
	public boolean closeCluster(YarnCluster cluster) {
		for (Entry<ClusterInfo, YarnCluster> entry : clusters.entrySet()) {
			if (entry.getValue().equals(cluster)) {
				entry.getValue().stop();
				clusters.remove(entry.getKey());
				return true;
			}
		}
		return false;
	}

	/**
	 * Close the manager, removes shutdown hook and
	 * closes all running clusters.
	 */
	public void close() {
		synchronized (this.startupShutdownMonitor) {
			doClose();
			// If we registered a JVM shutdown hook, we don't need it anymore now:
			// We've already explicitly closed the context.
			if (this.shutdownHook != null) {
				try {
					Runtime.getRuntime().removeShutdownHook(this.shutdownHook);
				}
				catch (IllegalStateException ex) {
					// ignore - VM is already shutting down
				}
			}
		}
	}

	/**
	 * Register a jvm shutdown hook allowing manager
	 * to gracefully shutdown clusters in case that
	 * hasn't already happened. This is usually the
	 * scenario in tests.
	 */
	public synchronized void registerShutdownHook() {
		if (this.shutdownHook == null) {
			// No shutdown hook registered yet.
			this.shutdownHook = new Thread() {
				@Override
				public void run() {
					doClose();
				}
			};
			Runtime.getRuntime().addShutdownHook(this.shutdownHook);
		}
	}

	/**
	 * Bring down and un-register all the running clusters.
	 */
	protected void doClose() {
		for (Entry<ClusterInfo, YarnCluster> entry : clusters.entrySet()) {
			entry.getValue().stop();
		}
		clusters.clear();
	}

}
