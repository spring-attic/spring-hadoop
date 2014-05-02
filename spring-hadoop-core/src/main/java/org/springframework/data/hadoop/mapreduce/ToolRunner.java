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
package org.springframework.data.hadoop.mapreduce;

import java.util.Collection;
import java.util.concurrent.Callable;

import org.apache.hadoop.util.Tool;
import org.springframework.beans.factory.InitializingBean;

/**
 * Wrapper around {@link org.apache.hadoop.util.ToolRunner} allowing for an easier configuration and execution
 * of {@link Tool}  instances inside Spring.
 * Optionally returns the execution result (as an int per {@link Tool#run(String[])}).
 * <p>
 * To make the runner execute at startup, use {@link #setRunAtStartup(boolean)}.
 * 
 * @author Costin Leau
 */
public class ToolRunner extends ToolExecutor implements Callable<Integer>, InitializingBean {

	private boolean runAtStartup = false;

	private Iterable<Callable<?>> preActions;
	private Iterable<Callable<?>> postActions;

	@Override
	public Integer call() throws Exception {
		invoke(preActions);
		Integer result = runCode();
		invoke(postActions);
		return result;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();

		if (runAtStartup) {
			call();
		}
	}

	/**
	 * Indicates whether the tool should run at container startup (the default) or not.
	 *
	 * @param runAtStartup The runAtStartup to set.
	 */
	public void setRunAtStartup(boolean runAtStartup) {
		this.runAtStartup = runAtStartup;
	}

	/**
	 * Actions to be invoked before running the action.
	 * 
	 * @param actions actions
	 */
	public void setPreAction(Collection<Callable<?>> actions) {
		this.preActions = actions;
	}

	/**
	 * Actions to be invoked after running the action.
	 * 
	 * @param actions actions
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