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
package org.springframework.data.hadoop.test.junit;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.hadoop.test.context.HadoopCluster;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Abstract base class providing default functionality
 * for running tests using Hadoop mini cluster.
 *
 * @author Janne Valkealahti
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
public abstract class AbstractHadoopClusterTests implements ApplicationContextAware {

	protected ApplicationContext applicationContext;

	protected Configuration configuration;

	protected HadoopCluster hadoopCluster;

	/**
	 * Gets the {@link ApplicationContext} for tests.
	 *
	 * @return the Application context
	 */
	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	/**
	 * Gets the running cluster runtime
	 * {@link Configuration} for tests.
	 *
	 * @return the Hadoop cluster config
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Sets the {@link Configuration}.
	 *
	 * @param configuration the Configuration
	 */
	@Autowired
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Gets the running {@link HadoopCluster} for tests.
	 *
	 * @return the Hadoop cluster
	 */
	public HadoopCluster getHadoopCluster() {
		return hadoopCluster;
	}

	/**
	 * Sets the {@link HadoopCluster}
	 *
	 * @param hadoopCluster the Hadoop cluster
	 */
	@Autowired
	public void setHadoopCluster(HadoopCluster hadoopCluster) {
		this.hadoopCluster = hadoopCluster;
	}

	/**
	 * Returns a configured {@link FileSystem} instance for
	 * test cases to read and write files to it.
	 * <p>
	 * Tests should use this {@link FileSystem} instance.
	 *
	 * @return the filesystem configured by mini cluster.
	 * @throws IOException exception
	 */
	protected FileSystem getFileSystem() throws IOException {
		return hadoopCluster.getFileSystem();
	}

}
