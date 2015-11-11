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

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.springframework.data.hadoop.test.context.HadoopCluster;
import org.springframework.data.hadoop.test.support.compat.MiniMRClusterCompat;

/**
 * Standalone simple mini cluster having MR
 * and Hdfs nodes.
 *
 * @author Janne Valkealahti
 *
 */
public class StandaloneHadoopCluster implements HadoopCluster {

	private final static Log log = LogFactory.getLog(StandaloneHadoopCluster.class);

	/** MR specific mini cluster object via reflection. */
	private Object mrClusterObject;

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

	/** Number of nodes for dfs */
	private int nodes = 1;

	/**
	 * Instantiates a mini cluster with default
	 * cluster node count.
	 *
	 * @param clusterName the unique cluster name
	 */
	public StandaloneHadoopCluster(String clusterName) {
		this.clusterName = clusterName;
	}

	/**
	 * Instantiates a mini cluster with given
	 * cluster node count.
	 *
	 * @param clusterName the unique cluster name
	 * @param nodes the node count
	 */
	public StandaloneHadoopCluster(String clusterName, int nodes) {
		this.clusterName = clusterName;
		this.nodes = nodes;
	}

	@Override
	public Configuration getConfiguration() {
		return configuration;
	}

	@SuppressWarnings("deprecation")
	@Override
	public void start() throws IOException {
		log.info("Checking if cluster=" + clusterName + " needs to be started");
		synchronized (this.startupShutdownMonitor) {
			if (started) {
				return;
			}

			// TODO: fix for MAPREDUCE-2785
			// I don't like it at all to do it like this, but
			// until we find a better way for the fix,
			// just set the system property
			// who knows what kind of problems this will cause!!!
			// keeping this here as reminder for the next guy who
			// clean up the mess
			String tmpDir = getTmpDir();
			System.setProperty("hadoop.log.dir", tmpDir);

			// need to get unique dir per cluster
			System.setProperty("test.build.data", "build/test/data/" + clusterName);

			log.info("Starting cluster=" + clusterName);

			Configuration config = new JobConf();

			// umask trick
			String umask = getCurrentUmask(tmpDir, config);
			if (umask != null) {
				log.info("Setting expected umask to " + umask);
				config.set("dfs.datanode.data.dir.perm", umask);
			}

			// dfs cluster is updating config
			// newer dfs cluster are using builder pattern
			// but we need to support older versions in
			// a same code. there are some ramifications if
			// deprecated methods are removed in a future
			dfsCluster = new MiniDFSCluster(config, nodes, true, null);

			// we need to ask the config from mr cluster after init
			// returns. for this case it is not guaranteed that passed config
			// is updated.
			// we do all this via compatibility class which uses
			// reflection because at this point we don't know
			// the exact runtime implementation

			FileSystem fs = dfsCluster.getFileSystem();
			log.info("Dfs cluster uri= " + fs.getUri());

			mrClusterObject = MiniMRClusterCompat.instantiateCluster(this.getClass(),
					clusterName, nodes, config, fs, this.getClass().getClassLoader());

			configuration = MiniMRClusterCompat.getConfiguration(mrClusterObject);

			// set default uri again in case it wasn't updated
			FileSystem.setDefaultUri(configuration, fs.getUri());

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

			if (mrClusterObject != null) {
				MiniMRClusterCompat.stopCluster(mrClusterObject);
				mrClusterObject = null;
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
	public FileSystem getFileSystem() throws IOException {
		return dfsCluster != null ? dfsCluster.getFileSystem() : null;
	}

	/**
	 * Sets a number of nodes for cluster. Every node
	 * will act as mr and dfs role. Default is one node.
	 *
	 * @param nodes the number of nodes
	 */
	public void setNodes(int nodes) {
		this.nodes = nodes;
	}

	/**
	 * Tries to get and create a tmp directory.
	 *
	 * @return the absolute path to tmp directory
	 */
	private static String getTmpDir() {
	    String propTmpPath = System.getProperty("java.io.tmpdir");
	    Random random = new Random();
	    int rand = 1 + random.nextInt();
	    File tmpDir = new File(propTmpPath + File.separator + "hadoopTmpDir" + rand);
	    if (tmpDir.exists() == false) {
	        tmpDir.mkdir();
	    }
	    tmpDir.deleteOnExit();
	    return tmpDir.getAbsolutePath();
	}

	/**
	 * Gets the current umask.
	 */
	private String getCurrentUmask(String tmpDir, Configuration config) throws IOException {
		try {
			LocalFileSystem localFS = FileSystem.getLocal(config);
			return Integer.toOctalString(localFS.getFileStatus(new Path(getTmpDir())).getPermission().toShort());
		} catch (Exception e) {
			return null;
		}
	}

}
