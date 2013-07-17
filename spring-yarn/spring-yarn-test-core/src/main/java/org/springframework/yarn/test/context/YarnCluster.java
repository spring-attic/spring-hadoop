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
package org.springframework.yarn.test.context;

import java.io.File;

import org.apache.hadoop.conf.Configuration;

/**
 * Interface for Yarn miniclusters.
 * 
 * @author Janne Valkealahti
 *
 */
public interface YarnCluster {

	/**
	 * Gets the {@link Configuration} for the cluster.
	 * As most of the configuration parameters are not
	 * known until after cluster has been started, this
	 * configuration should be configured by the
	 * cluster itself.
	 * 
	 * @return the Cluster configured {@link Configuration}
	 */
	Configuration getConfiguration();
	
	/**
	 * Starts the cluster.
	 * 
	 * @throws Exception if cluster failed to start
	 */
	void start() throws Exception;
	
	/**
	 * Stops the cluster.
	 */
	void stop();
	
	/**
	 * Gets the working directory of Yarn nodes. This directory
	 * can be used to find log files of running containers.
	 * 
	 * @return Yarn working directory.
	 */
	File getYarnWorkDir();
	
}
