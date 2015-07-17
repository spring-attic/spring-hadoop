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


import java.sql.Connection;
import java.util.Collection;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.util.Assert;

import javax.sql.DataSource;

/**
 * FactoryBean for easy declaration and creation of a {@link HiveClient} using a {@link org.springframework.jdbc.core.JdbcTemplate}.
 *
 * The HiveClient class is not thread-safe. We use a {@link SingleConnectionDataSource} to hold on to a connection for
 * the duration of the client. This means that all operations while happen in the same session. This is important when
 * setting properties on the session.
 * 
 * Note that the caller needs to handle the object clean-up,  specifically calling {@link HiveClient#shutdown()}. 
 * 
 * In general, to avoid leaks it is recommended to use the {@link HiveTemplate}.
 * 
 * @author Costin Leau
 * @author Thomas Risberg
 */
public class HiveClientFactoryBean implements FactoryBean<HiveClientFactory>, InitializingBean, DisposableBean {

	private Collection<HiveScript> scripts;

	private DataSource hiveDataSource;

	private SingleConnectionDataSource factoryDataSource;

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(hiveDataSource, "HiveDataSource must be set");
		Connection con = DataSourceUtils.getConnection(hiveDataSource);
		factoryDataSource = new SingleConnectionDataSource(con, true);
	}

	@Override
	public void destroy() throws Exception {
		factoryDataSource.destroy();
	}

	private class DefaultHiveClientFactory implements HiveClientFactory {
		@Override
		public HiveClient getHiveClient() throws BeansException {
			try {
				return createHiveClient();
			} catch (Exception ex) {
				throw new BeanCreationException("Cannot create HiveClient instance", ex);
			}
		}
	}

	public HiveClientFactory getObject() {
		return new DefaultHiveClientFactory();
	}

	public Class<?> getObjectType() {
		return HiveClientFactory.class;
	}

	public boolean isSingleton() {
		return true;
	}

	protected HiveClient createHiveClient() {
		if (factoryDataSource == null) {
			throw new IllegalStateException("HiveDataSource must be set before requesting a HiveClient");
		}
		return new HiveClient(factoryDataSource);
	}

	public int getPhase() {
		return Integer.MIN_VALUE;
	}


	/**
	 * Sets the DataSource.
	 * 
	 * @param dataSource The DataSource.
	 */
	public void setHiveDataSource(DataSource dataSource) {
		this.hiveDataSource = dataSource;
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