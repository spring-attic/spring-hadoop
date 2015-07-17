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

import java.util.List;
import java.util.Map;

import org.springframework.dao.DataAccessException;


/**
 * Interface specifying a basic set of Hive operations. Implemented by {@link HiveTemplate}.
 * Not often used directly, but a useful option to enhance testability, as it can easily be mocked or stubbed. 
 * 
 * @author Costin Leau
 * @author Thomas Risberg
 */
public interface HiveOperations {

	/**
	 * Executes the action specified by the given callback object within an active {@link HiveClient}. 
	 * 
	 * @param action callback object taht specifies the Hive action
	 * @param <T> action type
	 * @return the action result object
	 * @throws DataAccessException exception
	 */
	<T> T execute(HiveClientCallback<T> action) throws DataAccessException;

	/**
	 * Executes the given HiveQL that results in a list of objects.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 *  
	 * @param query HiveQL
	 * @return list of values returned by the query
	 * @throws DataAccessException exception
	 */
	List<String> query(String query) throws DataAccessException;

	/**
	 * Executes the given HiveQL using the list of arguments, expecting a list of objects.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return list of values returned by the query
	 * @throws DataAccessException exception
	 */
	List<String> query(String query, Map<?, ?> arguments) throws DataAccessException;

	/**
	 * Executes the given HiveQL that results in a single object.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 * 
	 * @param query HiveQL
	 * @return query result
	 * @throws DataAccessException exception
	 */
	String queryForString(String query) throws DataAccessException;

	/**
	 * Executes the given HiveQL using the list of arguments, that results in a single object.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return query result
	 * @throws DataAccessException exception
	 */
	String queryForString(String query, Map<?, ?> arguments) throws DataAccessException;

	/**
	 * Executes the given HiveQL that results in a single int value.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 * 
	 * @param query HiveQL
	 * @return query int result
	 * @throws DataAccessException exception
	 */
	Integer queryForInt(String query) throws DataAccessException;

	/**
	 * Executes the given HiveQL using the list of arguments, that results in a single int value.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return query int result
	 * @throws DataAccessException exception
	 */
	Integer queryForInt(String query, Map<?, ?> arguments) throws DataAccessException;

	/**
	 * Executes the given HiveQL that results in a single long value.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 * 
	 * @param query HiveQL
	 * @return query long result
	 * @throws DataAccessException exception
	 */
	Long queryForLong(String query) throws DataAccessException;

	/**
	 * Executes the given HiveQL using the list of arguments, that results in a single long value.
	 * The script is interpreted as a URL or if that fails, as a HiveQL statement.
	 * 
	 * @param query HiveQL
	 * @param arguments query arguments
	 * @return query long result
	 * @throws DataAccessException exception
	 */
	Long queryForLong(String query, Map<?, ?> arguments) throws DataAccessException;

	/**
	 * Executes a Hive script.
	 * 
	 * @param script script resource and arguments
	 * @return script result
	 * @throws DataAccessException exception
	 */
	List<String> executeScript(HiveScript script) throws DataAccessException;

	/**
	 * Executes multiple Hive scripts.
	 * 
	 * @param scripts script resources and arguments
	 * @return scripts results
	 * @throws DataAccessException exception
	 */
	List<String> executeScript(Iterable<HiveScript> scripts) throws DataAccessException;

}