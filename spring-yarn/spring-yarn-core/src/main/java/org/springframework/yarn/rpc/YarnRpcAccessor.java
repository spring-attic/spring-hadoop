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
package org.springframework.yarn.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;
import org.springframework.yarn.support.YarnUtils;

/**
 * Base implementation for accessing yarn components over
 * protocol buffer rpc system.
 *
 * @author Janne Valkealahti
 *
 * @param <P> Type of the protocol buffer implementation
 */
public abstract class YarnRpcAccessor<P> implements InitializingBean, DisposableBean {

	/** Protocol class used to create rpc connection */
	private Class<P> protocolClazz;

	/** Yarn configuration */
	private Configuration configuration;

	/** Address for rpc end point */
	private InetSocketAddress address;

	/** Created proxy */
	private P proxy;

	/**
	 * Instantiates a new yarn rpc accessor with a protocol class
	 * and Yarn configuration.
	 *
	 * @param protocolClazz the protocol clazz
	 * @param config the yarn configuration
	 */
	public YarnRpcAccessor(Class<P> protocolClazz, Configuration config) {
		this.protocolClazz = protocolClazz;
		this.configuration = config;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(configuration, "Yarn configuration must be set");
		Assert.notNull(protocolClazz, "Rpc protocol class must be set");
		if (UserGroupInformation.isSecurityEnabled()) {
			UserGroupInformation.setConfiguration(configuration);
		}
		address = getRpcAddress(configuration);
		proxy = createProxy();
	}

	@Override
	public void destroy() {
		RPC.stopProxy(proxy);
	}

	/**
	 * Gets the proxy handled by this accessor.
	 *
	 * @return the proxy
	 */
	public P getProxy() {
		return proxy;
	}

	/**
	 * Execute given action callback on the rpc proxy.
	 *
	 * @param <T> the return type
	 * @param <S> the proxy type
	 * @param action the action
	 * @return the result from a callback execution
	 * @throws DataAccessException the data access exception
	 */
	public <T, S extends P> T execute(YarnRpcCallback<T, S> action) throws DataAccessException {
		@SuppressWarnings("unchecked")
		S proxy = (S) getProxy();
		try {
			T result = action.doInYarn(proxy);
			return result;
		} catch (YarnException e) {
			throw YarnUtils.convertYarnAccessException(e);
		} catch (YarnRuntimeException e) {
			throw YarnUtils.convertYarnAccessException(e);
		} catch (IOException e) {
			throw YarnUtils.convertYarnAccessException(e);
		} catch (RuntimeException e) {
			throw e;
		}
	}

	/**
	 * Gets the Yarn configuration.
	 *
	 * @return the Yarn configuration
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Creates the proxy. If {@link #getUser()} returns
	 * a non null {@link UserGroupInformation user}, that
	 * will be used to request the proxy with
	 * a {@link PrivilegedAction}.
	 *
	 * @return the proxy
	 * @throws IOException if creation fails
	 */
	@SuppressWarnings("unchecked")
	protected P createProxy() throws IOException {
		return getUser().doAs(new PrivilegedAction<P>() {
			@Override
			public P run() {
				return (P) YarnRPC.create(configuration).getProxy(protocolClazz, address, configuration);
			}
		});
	}

	/**
	 * Gets the {@link UserGroupInformation user} used to
	 * create the proxy. Default implementation delegates into
	 * {@link UserGroupInformation#getCurrentUser()}.
	 *
	 * @return the user
	 * @throws IOException if login fails
	 * @see #createProxy()
	 */
	protected UserGroupInformation getUser() throws IOException {
		return UserGroupInformation.getCurrentUser();
	}

	/**
	 * Gets the {@link InetSocketAddress} where this accessor should connect.
	 *
	 * @param configuration the yarn configuration
	 * @return address of rpc endpoint
	 */
	protected abstract InetSocketAddress getRpcAddress(Configuration configuration);

}
