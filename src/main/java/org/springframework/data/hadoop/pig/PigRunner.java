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

import java.util.List;

import org.apache.pig.backend.executionengine.ExecJob;
import org.springframework.beans.factory.FactoryBean;

/**
 * Basic runner of Pig scripts inside a Spring environment. For more advanced functionality, consider using Spring Batch and the {@link PigTasklet}.
 * 
 * <p/>Note by default, the runner is configured to execute at startup. One can customize this behaviour through {@link #setRunAtStartup(boolean)}.
 * 
 * <p/>This class is a factory bean - if {@link #setRunAtStartup(boolean)} is set to false, then the action (namely the execution of the Pig scripts) is postponed until
 * {@link #getObject()} is called.
 *
 * @author Costin Leau
 */
public class PigRunner extends PigExecutor implements FactoryBean<List<ExecJob>> {

	private boolean runAtStartup = false;
	private volatile List<ExecJob> result = null;

	@Override
	public void afterPropertiesSet() {
		super.afterPropertiesSet();

		if (runAtStartup) {
			getObject();
		}
	}

	@Override
	public List<ExecJob> getObject() {
		if (result == null) {
			result = executePigScripts();
		}
		return result;
	}

	@Override
	public Class<?> getObjectType() {
		return List.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	/**
	 * Indicates whether the scripts should run at container startup or not (the default). 
	 *
	 * @param runAtStartup The runAtStartup to set.
	 */
	public void setRunAtStartup(boolean runAtStartup) {
		this.runAtStartup = runAtStartup;
	}
}