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
package org.springframework.data.hadoop.hive;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;

/**
 * Common class for configuring and executing Hive scripts.
 * Shared by the Hive tasklet and runner. 
 * 
 * @author Costin Leau
 */
public abstract class HiveExecutor implements InitializingBean {

	private HiveClientFactory hiveClientFactory;
	private HiveOperations hiveTemplate;
	private Collection<HiveScript> scripts;

	@Override
	public void afterPropertiesSet() throws Exception {
		if (hiveClientFactory == null && hiveTemplate == null) {
			throw new IllegalArgumentException("a HiveClient factory or a HiveTemplate is required");
		}

		if (hiveTemplate == null) {
			hiveTemplate = new HiveTemplate(hiveClientFactory);
		}
	}

	protected List<String> executeHiveScripts() {
		if (CollectionUtils.isEmpty(scripts)) {
			return Collections.emptyList();
		}

		return hiveTemplate.executeScript(scripts);
	}

	/**
	 * Sets the scripts to be executed by this tasklet.
	 * 
	 * @param scripts The scripts to set.
	 */
	public void setScripts(Collection<HiveScript> scripts) {
		this.scripts = scripts;
	}

	/**
	 * Sets the hive client for this tasklet.
	 *  
	 * @param hiveClientFactory hiveFactory to set
	 */
	public void setHiveClientFactory(HiveClientFactory hiveClientFactory) {
		this.hiveClientFactory = hiveClientFactory;
	}

	/**
	 * Sets the hive template.
	 *
	 * @param hiveTemplate the new hive template
	 */
	public void setHiveTemplate(HiveOperations hiveTemplate) {
		this.hiveTemplate = hiveTemplate;
	}
}
