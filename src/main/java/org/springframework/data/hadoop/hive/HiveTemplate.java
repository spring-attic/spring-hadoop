/*
 * Copyright 2011-2012 the original author or authors.
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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.service.HiveClient;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.TypeMismatchDataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.util.Assert;

/**
 * Helper class that simplifies Hive data access code. Automatically handles the creation of a {@link HiveClient} (which is non-thread-safe) 
 * and converts Hive exceptions into DataAccessExceptions.
 *
 * @author Costin Leau
 */
public class HiveTemplate implements InitializingBean, HiveOperations {

	private ObjectFactory<HiveClient> hiveClientFactory;


	/**
	 * Constructs a new <code>HiveClient</code> instance.
	 * Expects {@link #setHiveClient(ObjectFactory)} to be called before using it.
	 */
	public HiveTemplate() {
	}

	/**
	 * Constructs a new <code>HiveTemplate</code> instance.
	 *
	 * @param pigFactory pig factory
	 */
	public HiveTemplate(ObjectFactory<HiveClient> hiveClientFactory) {
		this.hiveClientFactory = hiveClientFactory;
		afterPropertiesSet();
	}

	@Override
	public void afterPropertiesSet() {
		Assert.notNull(hiveClientFactory, "non-null hive client factory required");
	}

	/**
	 * Executes the action specified by the given callback object within an active {@link HiveClient}. 
	 * 
	 * @param action callback object that specifies the Hive action
	 * @return the action result object
	 * @throws DataAccessException
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
				// ignore for now
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
	 *  
	 * @param query HiveQL
	 * @return list of values returned by the query
	 * @throws DataAccessException
	 */
	@Override
	public List<String> query(String query) throws DataAccessException {
		return query(query, null);
	}

	/**
	 * Executes the given HiveQL using the list of arguments, expecting a list of objects.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return list of values returned by the query
	 * @throws DataAccessException
	 */
	@Override
	public List<String> query(String query, Map<String, String> arguments) throws DataAccessException {
		return executeScript(new HiveScript(new ByteArrayResource(query.getBytes()), arguments));
	}

	/**
	 * Executes the given HiveQL that results in a single object.
	 * 
	 * @param query HiveQL
	 * @return query result
	 * @throws DataAccessException
	 */
	@Override
	public String queryForString(String query) throws DataAccessException {
		return queryForString(query, null);
	}

	/**
	 * Executes the given HiveQL using the list of arguments, that results in a single object.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return query result
	 * @throws DataAccessException
	 */
	@Override
	public String queryForString(String query, Map<String, String> arguments) throws DataAccessException {
		return DataAccessUtils.singleResult(executeScript(new HiveScript(new ByteArrayResource(query.getBytes()), arguments)));
	}

	/**
	 * Executes the given HiveQL that results in a single int value.
	 * 
	 * @param query HiveQL
	 * @return query int result
	 * @throws DataAccessException
	 */
	@Override
	public Integer queryForInt(String query) throws DataAccessException {
		return queryForInt(query, null);
	}

	/**
	 * Executes the given HiveQL using the list of arguments, that results in a single int value.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return query int result
	 * @throws DataAccessException
	 */
	@Override
	public Integer queryForInt(String query, Map<String, String> arguments) throws DataAccessException {
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
	 * 
	 * @param query HiveQL
	 * @return query long result
	 * @throws DataAccessException
	 */
	@Override
	public Long queryForLong(String query) throws DataAccessException {
		return queryForLong(query, null);
	}

	/**
	 * Executes the given HiveQL using the list of arguments, that results in a single long value.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return query long result
	 * @throws DataAccessException
	 */
	@Override
	public Long queryForLong(String query, Map<String, String> arguments) throws DataAccessException {
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
	 * @param script script resource
	 * @return script result
	 * @throws DataAccessException
	 */
	@Override
	public List<String> executeScript(Resource script) throws DataAccessException {
		return executeScript(new HiveScript(script));
	}

	/**
	 * Executes a Hive script.
	 * 
	 * @param script script resource and arguments
	 * @return script result
	 * @throws DataAccessException
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
	 * @throws DataAccessException
	 */
	@Override
	public List<String> executeScript(Iterable<HiveScript> scripts) throws DataAccessException {
		return HiveUtils.run(createHiveClient(), scripts, true);
	}

	protected HiveClient createHiveClient() {
		return hiveClientFactory.getObject();
	}

	/**
	 * Sets the {@link HiveClient} factory.
	 * 
	 * @param hiveClientFactory
	 */
	public void setHiveClient(ObjectFactory<HiveClient> hiveClientFactory) {
		this.hiveClientFactory = hiveClientFactory;
	}
}