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
package org.springframework.data.hadoop.hbase;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;

/**
 * Factory for creating HBase specific configuration. By default also cleans up any connection associated with the current configuration.
 * 
 * 
 * @see HConnectionManager
 * @author Costin Leau
 */
public class HbaseConfigurationFactoryBean implements InitializingBean, DisposableBean, FactoryBean<Configuration> {

	private boolean deleteConnection = true;
	private boolean stopProxy = true;
	private Configuration config;
	private Configuration configuration;
	private Properties properties;

	/**
	 * Indicates whether the potential connection created by this config is destroyed at shutdown (default).
	 * 
	 * @param deleteConnection The deleteConnection to set.
	 */
	public void setDeleteConnection(boolean deleteConnection) {
		this.deleteConnection = deleteConnection;
	}

	/**
	 * Indicates whether, when/if the associated connection is destroyed, whether the proxy is stopped or not. 
	 * 
	 * @param stopProxy The stopProxy to set.
	 */
	public void setStopProxy(boolean stopProxy) {
		this.stopProxy = stopProxy;
	}

	/**
	 * Sets the Hadoop configuration to use.
	 * 
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public void destroy() {
		if (deleteConnection) {
			HConnectionManager.deleteConnection(getObject(), stopProxy);
		}
	}

	/**
	 * @param properties The properties to set.
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public void afterPropertiesSet() {
		config = (configuration != null ? HBaseConfiguration.create(configuration) : HBaseConfiguration.create());
		ConfigurationUtils.addProperties(config, properties);
	}

	public Configuration getObject() {
		return config;
	}

	public Class<? extends Configuration> getObjectType() {
		return (config != null ? config.getClass() : Configuration.class);
	}

	public boolean isSingleton() {
		return true;
	}
}