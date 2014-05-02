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
import org.springframework.util.StringUtils;

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
	private String user;
	private boolean closeAll = false;
	private boolean close = true;

	public void afterPropertiesSet() throws Exception {
		Configuration cfg = (configuration != null ? configuration : new Configuration(true));
		if (uri == null) {
			uri = FileSystem.getDefaultUri(cfg);
		}
		if (StringUtils.hasText(user)) {
			fs = FileSystem.get(uri, cfg, user);
		}
		else {
			fs = FileSystem.get(uri, cfg);
		}
	}

	public void destroy() throws Exception {
		if (fs != null && close) {
			fs.close();
		}
		fs = null;

		if (closeAll) {
			// TODO: potentially call close all just for the current user
			FileSystem.closeAll();
		}
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
	 * Sets the Hadoop configuration for this file system.
	 * 
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Sets the URI (if available) for this file system.
	 * 
	 * @param uri The uri to set.
	 */
	public void setUri(URI uri) {
		this.uri = uri;
	}

	/**
	 * Sets the user impersonation (optional) for creating this file-system.
	 * Should be used when running against a Hadoop Kerberos cluster. 
	 * 
	 * @param user user/group information
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * Indicates whether all the Hadoop file systems should be closed once this factory is destroyed.
	 * False by default - should be turned on as a safety measure when the app controls the entire lifecycle of Hadoop inside
	 * a JVM.
	 *
	 * @param closeAll the close all
	 * @see FileSystem#closeAll()
	 */
	public void setCloseAll(boolean closeAll) {
		this.closeAll = closeAll;
	}

	/**
	 * Indicates whether the Hadoop file systems should be closed once this factory is destroyed.
	 * True by default - should be turned off when running 'embedded' or if long running operations outlive the application context.
	 *
	 * @param close close
	 * @see FileSystem#close()
	 */
	public void setClose(boolean close) {
		this.close = close;
	}
}