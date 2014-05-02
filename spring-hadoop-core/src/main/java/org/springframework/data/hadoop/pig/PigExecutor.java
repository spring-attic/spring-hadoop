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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecJob;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;

/**
 * Common class for configuring and executing pig scripts.
 * Shared by the Pig tasklet and runner.
 *  
 * @author Costin Leau
 */
public abstract class PigExecutor implements InitializingBean {

	private PigServerFactory pigFactory;
	private PigOperations pigTemplate;
	private Collection<PigScript> scripts;


	@Override
	public void afterPropertiesSet() throws Exception {
		if (pigFactory == null && pigTemplate == null) {
			throw new IllegalArgumentException("a PigServer factory or a PigTemplate is required");
		}

		if (pigTemplate == null) {
			pigTemplate = new PigTemplate(pigFactory);
		}
	}

	protected List<ExecJob> executePigScripts() {
		if (CollectionUtils.isEmpty(scripts)) {
			return Collections.emptyList();
		}
		return pigTemplate.executeScript(scripts);
	}

	/**
	 * Sets the pig scripts to be executed by this class.
	 * 
	 * @param scripts The scripts to set.
	 */
	public void setScripts(Collection<PigScript> scripts) {
		this.scripts = scripts;
	}

	/**
	 * Sets the pig server instance used by this class.
	 * 
	 * @param pigFactory The pigFactory to set.
	 */
	public void setPigFactory(PigServerFactory pigFactory) {
		this.pigFactory = pigFactory;
	}

	/**
	 * Sets the pig template used by this class.
	 * @param pigTemplate the pigTemplate
	 */
	public void setPigTemplate(PigOperations pigTemplate) {
		this.pigTemplate = pigTemplate;
	}
}