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
import org.springframework.util.Assert;

/**
 * Helper class that simplifies Hive data access code. Automatically handles the creation of a {@link HiveClient} (which is non-thread-safe) 
 * and converts Hive exceptions into DataAccessExceptions.
 *
 * @author Costin Leau
 */
public class HiveTemplate implements InitializingBean {

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

	public List<String> execute(String script) throws DataAccessException {
		return execute(script, null);
	}

	public List<String> execute(String script, Map<String, String> arguments) throws DataAccessException {
		return execute(new HiveScript(new ByteArrayResource(script.getBytes()), arguments));
	}

	public List<String> execute(Resource script) throws DataAccessException {
		return execute(new HiveScript(script));
	}

	public List<String> execute(HiveScript script) throws DataAccessException {
		return execute(Collections.singleton(script));
	}

	public List<String> execute(Iterable<HiveScript> scripts) throws DataAccessException {
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