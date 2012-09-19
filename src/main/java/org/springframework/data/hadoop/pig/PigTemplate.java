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
package org.springframework.data.hadoop.pig;

import java.util.Collections;
import java.util.List;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;

/**
 * Helper class that simplifies Pig data access code. Automatically handles the creation of a PigServer (which is non-thread-safe) 
 * and converts Pig exceptions into DataAccessExceptions.
 *  
 * @author Costin Leau
 */
public class PigTemplate implements InitializingBean {

	private ObjectFactory<PigServer> pigServerFactory;

	public PigTemplate() {
	}

	public PigTemplate(ObjectFactory<PigServer> pigFactory) {
		this.pigServerFactory = pigFactory;
		afterPropertiesSet();
	}


	@Override
	public void afterPropertiesSet() {
		Assert.notNull(pigServerFactory, "non-null pig server factory required");
	}

	public List<ExecJob> execute(Resource script) throws DataAccessException {
		return execute(new PigScript(script));
	}

	public List<ExecJob> execute(PigScript script) throws DataAccessException {
		return execute(Collections.singleton(script));
	}

	public List<ExecJob> execute(Iterable<PigScript> scripts) throws DataAccessException {
		return PigUtils.run(createPigServer(), scripts);
	}

	protected PigServer createPigServer() {
		return pigServerFactory.getObject();
	}

	/**
	 * Sets the {@link PigServer} factory.
	 * 
	 * @param pigServerFactory
	 */
	public void setPigServer(ObjectFactory<PigServer> pigServerFactory) {
		this.pigServerFactory = pigServerFactory;
	}
}