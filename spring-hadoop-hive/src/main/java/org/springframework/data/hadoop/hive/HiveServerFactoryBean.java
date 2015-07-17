/*
 * Copyright 2011-2015 the original author or authors.
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
package org.springframework.data.hadoop.hive;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hive.service.server.HiveServer2;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;

/**
 * FactoryBean for easy declaration and creation of an embeddable Hive server. 
 * Similar in functionality to {@link HiveServer2} but not tied to the command line.
 *
 * This class is intended for using as part of integration testing. For production use
 * a Hiveserver2 running on the Hadoop cluster is preferred.
 * 
 * @author Costin Leau
 * @author Thomas Risberg
 */
public class HiveServerFactoryBean implements FactoryBean<HiveServer2>, InitializingBean, DisposableBean, SmartLifecycle {

	private boolean autoStartup = true;
	private HiveServer2 server;
	private String host = "localhost";
	private int port = 10000;
	private int minThreads = 10, maxThreads = 100;
	private Configuration configuration;
	private Properties properties;
	private HiveConf hiveConf;

	@Override
	public void destroy() {
		stop();
		CommandProcessorFactory.clean(hiveConf);
		server = null;
	}

	@Override
	public void afterPropertiesSet() throws Exception {

		Configuration cfg = ConfigurationUtils.createFrom(configuration, properties);
		hiveConf = new HiveConf(cfg, HiveServer2.class);
		hiveConf.set("hive.server2.thrift.bind.host", host);
		hiveConf.set("hive.server2.thrift.port", String.valueOf(port));
		hiveConf.set("hive.server2.thrift.min.worker.threads", String.valueOf(minThreads));
		hiveConf.set("hive.server2.thrift.max.worker.threads", String.valueOf(maxThreads));

		ServerUtils.cleanUpScratchDir(hiveConf);

		server = new HiveServer2();
		server.init(hiveConf);

		if (autoStartup) {
			server.start();
		}
	}

	@Override
	public HiveServer2 getObject() {
		return server;
	}

	@Override
	public Class<?> getObjectType() {
		return (server != null ? server.getClass() : HiveServer2.class);
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void start() {
		if (!isRunning()) {
			server.start();
		}
	}

	@Override
	public void stop() {
		if (isRunning()) {
			server.stop();
		}
	}

	@Override
	public boolean isRunning() {
		if (server != null && server.getServiceState().equals(HiveServer2.STATE.STARTED)) {
			return true;
		}
		return false;
	}

	@Override
	public int getPhase() {
		return Integer.MIN_VALUE;
	}

	@Override
	public boolean isAutoStartup() {
		return autoStartup;
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		callback.run();
	}

	/**
	 * Indicates whether the Hive server should start automatically (default) or not.
	 * 
	 * @param autoStartup whether to automatically start or not
	 */
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * Sets the server host to bind to.
	 *
	 * @param host The host to use.
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * Sets the server port.
	 * 
	 * @param port The port to set.
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Sets the minimum number of threads serving this server.
	 * 
	 * @param minThreads The minThreads to set.
	 */
	public void setMinThreads(int minThreads) {
		this.minThreads = minThreads;
	}

	/**
	 * Sets the maximum number of threads serving this server.
	 * 
	 * @param maxThreads The maxThreads to set.
	 */
	public void setMaxThreads(int maxThreads) {
		this.maxThreads = maxThreads;
	}

	/**
	 * Sets the Hadoop configuration to use.
	 * 
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Sets the configuration properties.
	 * 
	 * @param properties The properties to set.
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

}