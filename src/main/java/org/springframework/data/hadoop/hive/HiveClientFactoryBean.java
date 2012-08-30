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
import org.apache.thrift.transport.TTransportException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.HadoopException;
import org.springframework.util.CollectionUtils;

/**
 * FactoryBean for easy declaration and creation of a {@link HiveClient} using {@link ThriftHive}.
 * Since Thrift clients are not thread-safe, neither is HiveClient. Thus this factory bean creates a
 * new instance on each call which needs to be disposed by the using code, specifically calling
 * {@link HiveClient#shutdown()}
 * 
 * @author Costin Leau
 */
public class HiveClientFactoryBean implements FactoryBean<HiveClient> {

	private Collection<HiveScript> scripts;

	private String host = "localhost";
	private int port = 10000;
	private int timeout = 0;

	public HiveClient getObject() {
		return createHiveClient();
	}

	public Class<?> getObjectType() {
		return HiveClient.class;
	}

	public boolean isSingleton() {
		return false;
	}

	protected HiveClient createHiveClient() {
		TSocket transport = new TSocket(host, port, timeout);
		HiveClient hive = new HiveClient(new TBinaryProtocol(transport));

		Resource lastScript = null;
		try {
			transport.open();

			if (!CollectionUtils.isEmpty(scripts)) {
				for (HiveScript script : scripts) {
					lastScript = script.getResource();
					HiveScriptRunner.run(hive, script);
				}
			}

		} catch (TTransportException ex) {
			throw new BeanCreationException("Cannot start transport", ex);
		} catch (Exception ex) {
			throw new HadoopException("Cannot execute Hive script [" + lastScript.getDescription(), ex);
		}

		return hive;
	}

	public int getPhase() {
		return Integer.MIN_VALUE;
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
	public void setScripts(Collection<HiveScript> scripts) {
		this.scripts = scripts;
	}
}