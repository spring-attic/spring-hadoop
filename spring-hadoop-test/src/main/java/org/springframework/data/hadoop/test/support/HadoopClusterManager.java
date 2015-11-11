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

import java.util.Hashtable;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.hadoop.test.context.HadoopCluster;

/**
 * Manager handling running mini clusters for tests.
 *
 * @author Janne Valkealahti
 *
 */
public class HadoopClusterManager {

	private final static Log log = LogFactory.getLog(HadoopClusterManager.class);

	/** Singleton instance */
	private static volatile HadoopClusterManager instance = null;

	/** Synchronization monitor for the "refresh" and "destroy" */
	private final Object startupShutdownMonitor = new Object();

	/** Reference to the JVM shutdown hook, if registered */
	private Thread shutdownHook;

	/** Cached clusters*/
	private Hashtable<ClusterInfo, HadoopCluster> clusters = new Hashtable<ClusterInfo, HadoopCluster>();

	/**
	 * Private initializer for singleton access.
	 */
	private HadoopClusterManager() {}

	/**
	 * Gets the singleton instance of {@link HadoopClusterManager}.
	 *
	 * @return the singleton instance
	 */
	public static HadoopClusterManager getInstance() {
		if (instance == null) {
			synchronized (HadoopClusterManager.class) {
				if (instance == null) {
					instance = new HadoopClusterManager();
				}
			}
		}
		return instance;
	}

	/**
	 * Gets the singleton instance of {@link HadoopClusterManager}.
	 *
	 * @param registerShutdownHook if true register shutdown hook
	 * @return the singleton instance
	 */
	public static HadoopClusterManager getInstance(boolean registerShutdownHook) {
		HadoopClusterManager instance2 = getInstance();
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
	public HadoopCluster getCluster(ClusterInfo clusterInfo) {
		HadoopCluster cluster = clusters.get(clusterInfo);
		if (cluster == null) {
			log.info("Building new cluster for ClusterInfo=" + clusterInfo);
			try {
				HadoopClusterFactoryBean fb = new HadoopClusterFactoryBean();
				fb.setClusterId("hadoop-" + clusterInfo.getId() + "-" + clusterInfo.hashCode());
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
	 * Closes and remove {@link HadoopCluster} from
	 * a manager cache.
	 *
	 * @param cluster the Hadoop cluster
	 * @return true if cluster was closed, false otherwise
	 */
	public boolean closeCluster(HadoopCluster cluster) {
		for (Entry<ClusterInfo, HadoopCluster> entry : clusters.entrySet()) {
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
			// If we registered a JVM shutdown hook, we don't need it anymore:
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
					log.info("Received shutdown hook, about to call doClose()");
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
		log.info("Closing all clusters handled by this manager");
		for (Entry<ClusterInfo, HadoopCluster> entry : clusters.entrySet()) {
			entry.getValue().stop();
		}
		clusters.clear();
	}

}
