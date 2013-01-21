/*
 * Copyright 2011-2013 the original author or authors.
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
import java.util.concurrent.Executor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.hadoop.hive.service.HiveServer.HiveServerHandler;
import org.apache.hadoop.hive.service.HiveServer.ThriftHiveProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;

/**
 * FactoryBean for easy declaration and creation of an embeddable Hive server. 
 * Similar in functionality to {@link HiveServer} but not tied to the command line. 
 * 
 * @author Costin Leau
 */
public class HiveServerFactoryBean implements FactoryBean<TServer>, InitializingBean, DisposableBean, SmartLifecycle {

	private boolean autoStartup = true;
	private TServer server;
	private int port = 10000;
	private int minThreads = 10, maxThreads = 100;
	private Configuration configuration;
	private Executor executor;
	private Properties properties;
	private HiveConf conf;

	@Override
	public void destroy() {
		stop();
		CommandProcessorFactory.clean(conf);
		server = null;
	}

	@Override
	public void afterPropertiesSet() throws Exception {

		Configuration cfg = ConfigurationUtils.createFrom(configuration, properties);

		conf = new HiveConf(cfg, HiveServerHandler.class);

		ServerUtils.cleanUpScratchDir(conf);
		TServerTransport serverTransport = new TServerSocket(port);

		// Hive 0.8.0
		ThriftHiveProcessorFactory hfactory = new ThriftHiveProcessorFactory(null, conf);

		TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(serverTransport).processorFactory(hfactory).
				transportFactory(new TTransportFactory()).
				protocolFactory(new TBinaryProtocol.Factory()).
				minWorkerThreads(minThreads).
				maxWorkerThreads(maxThreads);
		server = new TThreadPoolServer(sargs);

		//	Hive 0.7.x (unfortunately it doesn't support passing a configuration object to it)
		// 
		//		ThriftHiveProcessorFactory hfactory = new ThriftHiveProcessorFactory(null);
		//		TThreadPoolServer.Options options = new TThreadPoolServer.Options();
		//		options.minWorkerThreads = minThreads;
		//		options.maxWorkerThreads = maxThreads;

		//		server = new TThreadPoolServer(hfactory, serverTransport, new TTransportFactory(), new TTransportFactory(),
		//				new TBinaryProtocol.Factory(), new TBinaryProtocol.Factory(), options);

		if (executor == null) {
			executor = new SimpleAsyncTaskExecutor(getClass().getSimpleName());
		}
	}

	@Override
	public TServer getObject() {
		return server;
	}

	@Override
	public Class<?> getObjectType() {
		return (server != null ? server.getClass() : TServer.class);
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void start() {
		if (!isRunning()) {
			executor.execute(new Runnable() {
				@Override
				public void run() {
					server.serve();
				}
			});
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
		return server.isServing();
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
	 * Indicates whether the Hive client should start automatically (default) or not.
	 * 
	 * @param autoStartup whether to automatically start or not
	 */
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
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

	/**
	 * Sets the executor for starting the server (which is a blocking operation).
	 * By default, an internal {@link SimpleAsyncTaskExecutor} instance is used.
	 * 
	 * @param executor The executor to set.
	 */
	public void setExecutor(Executor executor) {
		this.executor = executor;
	}
}