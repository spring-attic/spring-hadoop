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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.service.HiveClient;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;


/**
 * Interface specifying a basic set of Hive operations. Implemented by {@link HiveTemplate}.
 * Not often used directly, but a useful option to enhance testability, as it can easily be mocked or stubbed. 
 * 
 * @author Costin Leau
 */
public interface HiveOperations {

	/**
	 * Executes the action specified by the given callback object within an active {@link HiveClient}. 
	 * 
	 * @param action callback object taht specifies the Hive action
	 * @return the action result object
	 * @throws DataAccessException
	 */
	<T> T execute(HiveClientCallback<T> action) throws DataAccessException;

	/**
	 * Executes the given HiveQL that results in a list of objects.
	 *  
	 * @param query HiveQL
	 * @return list of values returned by the query
	 * @throws DataAccessException
	 */
	List<String> query(String query) throws DataAccessException;

	/**
	 * Executes the given HiveQL using the list of arguments, expecting a list of objects.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return list of values returned by the query
	 * @throws DataAccessException
	 */
	List<String> query(String query, Map<String, String> arguments) throws DataAccessException;

	/**
	 * Executes the given HiveQL that results in a single object.
	 * 
	 * @param query HiveQL
	 * @return query result
	 * @throws DataAccessException
	 */
	String queryForString(String query) throws DataAccessException;

	/**
	 * Executes the given HiveQL using the list of arguments, that results in a single object.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return query result
	 * @throws DataAccessException
	 */
	String queryForString(String query, Map<String, String> arguments) throws DataAccessException;

	/**
	 * Executes the given HiveQL that results in a single int value.
	 * 
	 * @param query HiveQL
	 * @return query int result
	 * @throws DataAccessException
	 */
	Integer queryForInt(String query) throws DataAccessException;

	/**
	 * Executes the given HiveQL using the list of arguments, that results in a single int value.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return query int result
	 * @throws DataAccessException
	 */
	Integer queryForInt(String query, Map<String, String> arguments) throws DataAccessException;

	/**
	 * Executes the given HiveQL that results in a single long value.
	 * 
	 * @param query HiveQL
	 * @return query long result
	 * @throws DataAccessException
	 */
	Long queryForLong(String query) throws DataAccessException;

	/**
	 * Executes the given HiveQL using the list of arguments, that results in a single long value.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return query long result
	 * @throws DataAccessException
	 */
	Long queryForLong(String query, Map<String, String> arguments) throws DataAccessException;

	/**
	 * Executes a Hive script.
	 * 
	 * @param script script resource
	 * @return script result
	 * @throws DataAccessException
	 */
	List<String> executeScript(Resource script) throws DataAccessException;

	/**
	 * Executes a Hive script.
	 * 
	 * @param script script resource and arguments
	 * @return script result
	 * @throws DataAccessException
	 */
	List<String> executeScript(HiveScript script) throws DataAccessException;

	/**
	 * Executes multiple Hive scripts.
	 * 
	 * @param scripts script resources and arguments
	 * @return scripts results
	 * @throws DataAccessException
	 */
	List<String> executeScript(Iterable<HiveScript> scripts) throws DataAccessException;

}