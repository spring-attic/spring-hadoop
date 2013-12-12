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

package org.springframework.data.hadoop.store.dataset;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.filesystem.FileSystemDatasetRepository;

/**
 * Factory class responsible for creating {@link DatasetRepository} instances. Primarily used in configuration code or
 * application contexts.
 *
 * @author Thomas Risberg
 * @since 2.0
 */
public class DatasetRepositoryFactory implements InitializingBean {

	private Configuration conf;

	private DatasetRepository repo;

	private String basePath = "/";

	/**
	 * The Hadoop configuraton to be used
	 * 
	 * @param configuration Hadoop configuration
	 */
	public void setConf(Configuration configuration) {
		this.conf = configuration;
	}

	/**
	 * The base path for the datasets in this repository. This combined with the Hadoop configuration 'fs.defaultNS'
	 * setting determines the actual full path used.
	 * 
	 * @param basePath the base path to use
	 */
	public void setBasePath(String basePath) {
		this.basePath = basePath;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(conf, "The configuration property is required");
		this.repo = new FileSystemDatasetRepository.Builder()
				.rootDirectory(new URI(basePath)).configuration(conf).get();
	}

	/**
	 * Get the {@link DatasetRepository}
	 * 
	 * @return the dataset repository
	 */
	public DatasetRepository getDatasetRepository() {
		return repo;
	}
}
