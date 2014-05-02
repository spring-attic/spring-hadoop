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
package org.springframework.data.hadoop.pig;

import java.util.List;
import java.util.Map;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.springframework.dao.DataAccessException;

/**
 * Interface specifying a basic set of Pig operations. Implemented by {@link PigTemplate}.
 * Not often used directly, but a useful option to enhance testability, as it can easily be mocked or stubbed. 
 *
 * @author Costin Leau
 */
public interface PigOperations {

	/**
	 * Executes the action specified by the given callback object within an active {@link PigServer}. 
	 * 
	 * @param action callback object that specifies the Hive action
	 * @param <T> action type
	 * @return the action result object
	 * @throws DataAccessException exception
	 */
	<T> T execute(PigCallback<T> action) throws DataAccessException;

	/**
	 * Executes the given Pig Latin that results in a list of job executions.
	 * The script is interpreted as a URL or if that fails, as a Pig Latin statement.
	 *  
	 * @param script script URL or pig latin statement
	 * @return list of job executions
	 * @throws DataAccessException exception
	 */
	List<ExecJob> executeScript(String script) throws DataAccessException;

	/**
	 * Executes the given Pig Latin with arguments that results in a list of job executions.
	 * The script is interpreted as a URL or if that fails, as a Pig Latin statement.
	 * 
	 * @param script script URL or pig latin statement
	 * @param arguments script arguments
	 * @return list of job executions
	 * @throws DataAccessException exception
	 */
	List<ExecJob> executeScript(String script, Map<?, ?> arguments) throws DataAccessException;

	/**
	 * Executes the given script identified by location and arguments that results in a list of job executions.
	 * 
	 * @param script script location and arguments
	 * @return list of job executions
	 * @throws DataAccessException exception
	 */
	List<ExecJob> executeScript(PigScript script) throws DataAccessException;

	/**
	 * Executes multiple scripts that result in a list of job executions.
	 * 
	 * @param scripts scripts location and arguments
	 * @return list of job executions
	 * @throws DataAccessException exception
	 */
	List<ExecJob> executeScript(Iterable<PigScript> scripts) throws DataAccessException;

}