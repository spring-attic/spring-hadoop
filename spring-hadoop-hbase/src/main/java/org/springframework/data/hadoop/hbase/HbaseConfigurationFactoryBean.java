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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;
import org.springframework.util.StringUtils;

import java.util.Properties;

/**
 * Factory for creating HBase specific configuration. By default cleans up any connection associated with the current configuration.
 *
 * @author Costin Leau
 */
public class HbaseConfigurationFactoryBean implements InitializingBean, DisposableBean, FactoryBean<Configuration> {

    private static final Log log = LogFactory.getLog(HbaseConfigurationFactoryBean.class);

	private boolean deleteConnection = true;
	private Configuration configuration;
	private Configuration hadoopConfig;
	private Properties properties;
	private String quorum;
	private Integer port;

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
        log.warn("Use of 'stopProxy' has been deprecated");
	}

	/**
	 * Sets the Hadoop configuration to use.
	 *
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.hadoopConfig = configuration;
	}

	/**
	 * Sets the configuration properties.
	 *
	 * @param properties The properties to set.
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	@Override
	public void afterPropertiesSet() {
		configuration = (hadoopConfig != null ? HBaseConfiguration.create(hadoopConfig) : HBaseConfiguration.create());
		ConfigurationUtils.addProperties(configuration, properties);

		// set host and port last to override any other properties
		if (StringUtils.hasText(quorum)) {
			configuration.set(HConstants.ZOOKEEPER_QUORUM, quorum.trim());
		}
		if (port != null) {
			configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, port.toString());
		}
	}

	@Override
	public Configuration getObject() {
		return configuration;
	}

	@Override
	public Class<? extends Configuration> getObjectType() {
		return (configuration != null ? configuration.getClass() : Configuration.class);
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	/**
	 * Sets the HBase Zookeeper Quorum host(s). If not specified, the default value (picked the the classpath) is used.
	 *
	 * @param quorum HBase ZK quorum hosts.
	 */
	public void setZkQuorum(String quorum) {
		this.quorum = quorum;
	}

	/**
	 * Sets the HBase Zookeeper port for clients to connect to. If not specified, the default value (picked from the classpath) is used.
	 *
	 * @param port HBase ZK client port.
	 */
	public void setZkPort(Integer port) {
		this.port = port;
	}

	@Override
	public void destroy() throws Exception {

	}
}