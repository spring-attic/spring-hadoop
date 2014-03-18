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
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Basic runner for Hive scripts inside a Spring environment. For more advanced functionality, consider using Spring Batch and the HiveTasklet.
 *
 * To make the runner execute at startup, use {@link #setRunAtStartup(boolean)}.
 *
 * @author Costin Leau
 */
public class HiveRunner extends HiveExecutor implements Callable<List<String>> {

	private boolean runAtStartup = false;

	private Iterable<Callable<?>> preActions;
	private Iterable<Callable<?>> postActions;


	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();

		if (runAtStartup) {
			call();
		}
	}

	@Override
	public List<String> call() throws Exception {
		invoke(preActions);
		List<String> result = executeHiveScripts();
		invoke(postActions);
		return result;
	}

	/**
	 * Indicates whether the scripts should run at container startup or not (the default).
	 *
	 * @param runAtStartup The runAtStartup to set.
	 */
	public void setRunAtStartup(boolean runAtStartup) {
		this.runAtStartup = runAtStartup;
	}

	/**
	 * Actions to be invoked before running the action.
	 *
	 * @param actions
	 */
	public void setPreAction(Collection<Callable<?>> actions) {
		this.preActions = actions;
	}

	/**
	 * Actions to be invoked after running the action.
	 *
	 * @param actions
	 */
	public void setPostAction(Collection<Callable<?>> actions) {
		this.postActions = actions;
	}

	private void invoke(Iterable<Callable<?>> actions) throws Exception {
		if (actions != null) {
			for (Callable<?> action : actions) {
				action.call();
			}
		}
	}
}