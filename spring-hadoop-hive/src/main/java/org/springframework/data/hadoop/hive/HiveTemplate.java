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

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.dao.TypeMismatchDataAccessException;
import org.springframework.dao.UncategorizedDataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;

/**
 * Helper class that simplifies Hive data access code. Automatically handles the creation of a {@link HiveClient}
 * and converts Hive exceptions into DataAccessExceptions.
 *
 * @author Costin Leau
 * @author Thomas Risberg
 */
public class HiveTemplate implements InitializingBean, HiveOperations, ResourceLoaderAware {

	private HiveClientFactory hiveClientFactory;
	private ResourceLoader resourceLoader;


	/**
	 * Constructs a new <code>HiveClient</code> instance.
	 * Expects {@link #setHiveClientFactory(HiveClientFactory)} to be called before using it.
	 */
	public HiveTemplate() {
	}

	/**
	 * Constructs a new <code>HiveTemplate</code> instance.
	 *
	 * @param hiveClientFactory HiveClient factory
	 */
	public HiveTemplate(HiveClientFactory hiveClientFactory) {
		this.hiveClientFactory = hiveClientFactory;
		afterPropertiesSet();
	}

	@Override
	public void afterPropertiesSet() {
		Assert.notNull(hiveClientFactory, "non-null hive client factory required");
	}

	/**
	 * Executes the action specified by the given callback object within an active connection.
	 * 
	 * @param action callback object that specifies the Hive action
	 * @return the action result object
	 * @throws DataAccessException exception
	 */
	@Override
	public <T> T execute(HiveClientCallback<T> action) throws DataAccessException {
		Assert.notNull(action, "a valid callback is required");
		HiveClient hiveClient = createHiveClient();
		try {
			return action.doInHive(hiveClient);
		} catch (Exception ex) {
			throw convertHiveAccessException(ex);
		} finally {
			try {
				hiveClient.shutdown();
			} catch (Exception ex) {
				throw new InvalidDataAccessResourceUsageException("Error while closing client connection", ex);
			}
		}
	}

	/**
	 * Converts the given Hive exception to an appropriate exception from the <tt>org.springframework.dao</tt> hierarchy.
	 * 
	 * @param ex hive exception
	 * @return a corresponding DataAccessException
	 */
	protected DataAccessException convertHiveAccessException(Exception ex) {
		return HiveUtils.convert(ex);
	}

	/**
	 * Executes the given HiveQL that results in a list of objects.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 *  
	 * @param query HiveQL
	 * @return list of values returned by the query
	 * @throws DataAccessException exception
	 */
	@Override
	public List<String> query(String query) throws DataAccessException {
		return query(query, null);
	}

	/**
	 * Executes the given HiveQL using the list of arguments, expecting a list of objects.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return list of values returned by the query
	 * @throws DataAccessException exception
	 */
	@Override
	public List<String> query(String query, Map<?, ?> arguments) throws DataAccessException {
		Assert.hasText(query, "a script is required");

		Resource res = null;

		if (ResourceUtils.isUrl(query)) {
			if (resourceLoader != null) {
				res = resourceLoader.getResource(query);
			}
		}
		else {
			res = new ByteArrayResource(query.getBytes());
		}

		return executeScript(new HiveScript(res, arguments));
	}

	/**
	 * Executes the given HiveQL that results in a single object.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 * 
	 * @param query HiveQL
	 * @return query result
	 * @throws DataAccessException exception
	 */
	@Override
	public String queryForString(String query) throws DataAccessException {
		return queryForString(query, null);
	}

	/**
	 * Executes the given HiveQL using the list of arguments, that results in a single object.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return query result
	 * @throws DataAccessException exception
	 */
	@Override
	public String queryForString(String query, Map<?, ?> arguments) throws DataAccessException {
		return DataAccessUtils.singleResult(query(query, arguments));
	}

	/**
	 * Executes the given HiveQL that results in a single int value.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 * 
	 * @param query HiveQL
	 * @return query int result
	 * @throws DataAccessException exception
	 */
	@Override
	public Integer queryForInt(String query) throws DataAccessException {
		return queryForInt(query, null);
	}

	/**
	 * Executes the given HiveQL using the list of arguments, that results in a single int value.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return query int result
	 * @throws DataAccessException exception
	 */
	@Override
	public Integer queryForInt(String query, Map<?, ?> arguments) throws DataAccessException {
		String result = queryForString(query, arguments);
		if (result != null) {
			try {
				return Integer.valueOf(result);
			} catch (NumberFormatException ex) {
				throw new TypeMismatchDataAccessException("Invalid int result found [" + result + "]", ex);
			}
		}
		return null;
	}

	/**
	 * Executes the given HiveQL that results in a single long value.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 * 
	 * @param query HiveQL
	 * @return query long result
	 * @throws DataAccessException exception
	 */
	@Override
	public Long queryForLong(String query) throws DataAccessException {
		return queryForLong(query, null);
	}

	/**
	 * Executes the given HiveQL using the list of arguments, that results in a single long value.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return query long result
	 * @throws DataAccessException exception
	 */
	@Override
	public Long queryForLong(String query, Map<?, ?> arguments) throws DataAccessException {
		String result = queryForString(query, arguments);
		if (result != null) {
			try {
				return Long.valueOf(result);
			} catch (NumberFormatException ex) {
				throw new TypeMismatchDataAccessException("Invalid long result found [" + result + "]", ex);
			}
		}
		return null;
	}


	/**
	 * Executes a Hive script.
	 * 
	 * @param script script resource and arguments
	 * @return script result
	 * @throws DataAccessException exception
	 */
	@Override
	public List<String> executeScript(HiveScript script) throws DataAccessException {
		return executeScript(Collections.singleton(script));
	}

	/**
	 * Executes multiple Hive scripts.
	 * 
	 * @param scripts scripts resources and arguments
	 * @return scripts results
	 * @throws DataAccessException exception
	 */
	@Override
	public List<String> executeScript(final Iterable<HiveScript> scripts) throws DataAccessException {
		return execute(new HiveClientCallback<List<String>>() {
			@Override
			public List<String> doInHive(HiveClient hiveClient) throws Exception {
				return HiveUtils.run(hiveClient, scripts);
			}
		});
	}

	protected HiveClient createHiveClient() {
		return hiveClientFactory.getHiveClient();
	}

	/**
	 * Sets the {@link HiveClient} factory.
	 * 
	 * @param hiveClientFactory hive client factory to set
	 */
	public void setHiveClientFactory(HiveClientFactory hiveClientFactory) {
		this.hiveClientFactory = hiveClientFactory;
	}

	@Override
	public void setResourceLoader(ResourceLoader resourceLoader) {
		this.resourceLoader = resourceLoader;
	}
}