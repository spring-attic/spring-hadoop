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
package org.springframework.data.hadoop.hive;


import java.util.Collection;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.HadoopException;
import org.springframework.util.CollectionUtils;

/**
 * FactoryBean for easy declaration and creation of a {@link HiveClient} using {@link ThriftHive}.
 * 
 * @author Costin Leau
 */
public class HiveClientFactoryBean implements SmartLifecycle, FactoryBean<HiveClient>, InitializingBean, DisposableBean {

	private Collection<Resource> scripts;

	private HiveClient hive;
	private String host = "localhost";
	private int port = 10000;
	private int timeout = 0;
	private boolean autoStartup = true;

	private TTransport transport;

	public void afterPropertiesSet() {
		transport = new TSocket(host, port, timeout);
		hive = new HiveClient(new TBinaryProtocol(transport));
	}

	public void destroy() {
		stop();
		hive = null;
		transport = null;
	}

	public HiveClient getObject() {
		return hive;
	}

	public Class<?> getObjectType() {
		return (hive != null ? hive.getClass() : HiveClient.class);
	}

	public boolean isSingleton() {
		return true;
	}

	public boolean isAutoStartup() {
		return autoStartup;
	}

	public void stop(Runnable callback) {
		stop();
		callback.run();
	}

	public boolean isRunning() {
		return (transport != null && transport.isOpen());
	}

	public void start() {
		if (!isRunning()) {
			try {
				transport.open();

				if (!CollectionUtils.isEmpty(scripts)) {
					HiveScriptRunner.run(hive, scripts);
				}

			} catch (TTransportException ex) {
				throw new BeanCreationException("Cannot start transport", ex);
			} catch (Exception ex) {
				throw new HadoopException("Cannot execute Hive script(s)", ex);
			}
		}
	}

	public void stop() {
		if (isRunning()) {
			try {
				transport.flush();
			} catch (TTransportException ex) {
				// ignore
			}
			transport.close();
		}
	}

	public int getPhase() {
		return Integer.MIN_VALUE;
	}

	/**
	 * Indicates whether the Hive client should start automatically (default) or not.
	 * 
	 * @param autoStart whether to automatically start or not
	 */
	public void setAutoStartup(boolean autoStart) {
		this.autoStartup = autoStart;
	}

	/**
	 * Returns the host.
	 *
	 * @return Returns the host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Sets the server host. 
	 * 
	 * @param host The host to set.
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * Returns the port.
	 *
	 * @return Returns the port
	 */
	public int getPort() {
		return port;
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
	 * Returns the timeout.
	 *
	 * @return Returns the timeout
	 */
	public int getTimeout() {
		return timeout;
	}

	/**
	 * Sets the connection timeout.
	 * 
	 * @param timeout The timeout to set.
	 */
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	/**
	 * Sets the scripts to execute once the client connects.
	 * 
	 * @param scripts The scripts to set.
	 */
	public void setScripts(Collection<Resource> scripts) {
		this.scripts = scripts;
	}
}