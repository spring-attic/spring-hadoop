/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.hadoop.fs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * FactoryBean for creating Hadoop {@link FileSystem} instances. Useful for interacting with
 * HDFS from outside a Map Reduce job. If no parameter is given, a file system using the installed
 * Hadoop defaults will be created.
 * 
 * @author Costin Leau
 */
public class FileSystemFactoryBean implements InitializingBean, DisposableBean, FactoryBean<FileSystem> {

	private FileSystem fs;
	private Configuration configuration;
	private URI uri;

	public void afterPropertiesSet() throws Exception {
		Configuration cfg = (configuration != null ? configuration : new Configuration(true));
		fs = (uri != null ? FileSystem.get(uri, cfg) : FileSystem.get(cfg));
	}

	public void destroy() throws Exception {
		if (fs != null) {
			fs.close();
		}
		fs = null;
	}


	public FileSystem getObject() throws Exception {
		return fs;
	}

	public Class<?> getObjectType() {
		return (fs != null ? fs.getClass() : FileSystem.class);
	}

	public boolean isSingleton() {
		return true;
	}

	/**
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * @param uri The uri to set.
	 */
	public void setUri(URI uri) {
		this.uri = uri;
	}
}