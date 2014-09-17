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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;

/**
 * Helper class that simplifies Pig data access code. Automatically handles the creation of a PigServer (which is non-thread-safe) 
 * and converts Pig exceptions into DataAccessExceptions.
 *  
 * @author Costin Leau
 */
public class PigTemplate implements InitializingBean, PigOperations, ResourceLoaderAware {

	private PigServerFactory pigServerFactory;
	private ResourceLoader resourceLoader;

	/**
	 * Constructs a new <code>PigTemplate</code> instance.
	 * Expects {@link #setPigFactory(PigServerFactory)} to be called before using it.
	 */
	public PigTemplate() {
	}

	/**
	 * Constructs a new <code>PigTemplate</code> instance.
	 *
	 * @param pigFactory pig factory
	 */
	public PigTemplate(PigServerFactory pigFactory) {
		this.pigServerFactory = pigFactory;
		afterPropertiesSet();
	}

	@Override
	public void afterPropertiesSet() {
		Assert.notNull(pigServerFactory, "non-null pig server factory required");
	}

	/**
	 * Executes the action specified by the given callback object within an active {@link PigServer}. 
	 * 
	 * @param action callback object that specifies the Hive action
	 * @return the action result object
	 * @throws DataAccessException exception
	 */
	@Override
	public <T> T execute(PigCallback<T> action) throws DataAccessException {
		Assert.notNull(action, "a valid callback is required");
		PigServer pig = createPigServer();

		try {
			// make sure pig is connected
			pig.getPigContext().connect();
			return action.doInPig(pig);

		} catch (ExecException ex) {
			throw convertPigAccessException(ex);
		} catch (IOException ex) {
			throw convertPigAccessException(ex);
		} finally {
			pig.shutdown();
		}
	}

	/**
	 * Converts the given Pig exception to an appropriate exception from the <tt>org.springframework.dao</tt> hierarchy.
	 * 
	 * @param ex Pig exception
	 * @return a corresponding DataAccessException
	 */
	protected DataAccessException convertPigAccessException(IOException ex) {
		return PigUtils.convert(ex);
	}

	/**
	 * Converts the given Pig exception to an appropriate exception from the <tt>org.springframework.dao</tt> hierarchy.
	 * 
	 * @param ex Pig exception
	 * @return a corresponding DataAccessException
	 */
	protected DataAccessException convertPigAccessException(ExecException ex) {
		return PigUtils.convert(ex);
	}

	/**
	 * Executes the given Pig Latin that results in a list of job executions.
	 * The script is interpreted as a URL or if that fails, as a Pig Latin statement.
	 *  
	 * @param script script URL or pig latin statement
	 * @return list of job executions
	 * @throws DataAccessException exception
	 */
	@Override
	public List<ExecJob> executeScript(String script) throws DataAccessException {
		return executeScript(script, null);
	}

	/**
	 * Executes the given Pig Latin with arguments that results in a list of job executions.
	 * The script is interpreted as a URL or if that fails, as a Pig Latin statement.
	 * 
	 * @param script script URL or pig latin statement
	 * @param arguments script arguments
	 * @return list of job executions
	 * @throws DataAccessException exception
	 */
	@Override
	public List<ExecJob> executeScript(String script, Map<?, ?> arguments) throws DataAccessException {
		Assert.hasText(script, "a script is required");

		Resource res = null;

		if (ResourceUtils.isUrl(script)) {
			if (resourceLoader != null) {
				res = resourceLoader.getResource(script);
			}
		}
		else {
			res = new ByteArrayResource(script.getBytes());
		}

		return executeScript(new PigScript(res, arguments));
	}

	/**
	 * Executes the given script identified by location and arguments that results in a list of job executions.
	 * 
	 * @param script script location and arguments
	 * @return list of job executions
	 * @throws DataAccessException exception
	 */
	@Override
	public List<ExecJob> executeScript(PigScript script) throws DataAccessException {
		return executeScript(Collections.singleton(script));
	}

	/**
	 * Executes multiple scripts that result in a list of job executions.
	 * 
	 * @param scripts scripts location and arguments
	 * @return list of job executions
	 * @throws DataAccessException exception
	 */
	@Override
	public List<ExecJob> executeScript(final Iterable<PigScript> scripts) throws DataAccessException {
		return execute(new PigCallback<List<ExecJob>>() {
			@Override
			public List<ExecJob> doInPig(PigServer pig) throws ExecException, IOException {
				return PigUtils.run(createPigServer(), scripts);
			}
		});
	}

	protected PigServer createPigServer() {
		return pigServerFactory.getPigServer();
	}

	/**
	 * Sets the {@link PigServer} factory.
	 * 
	 * @param pigServerFactory pig server factory
	 */
	public void setPigFactory(PigServerFactory pigServerFactory) {
		this.pigServerFactory = pigServerFactory;
	}

	@Override
	public void setResourceLoader(ResourceLoader resourceLoader) {
		this.resourceLoader = resourceLoader;
	}
}