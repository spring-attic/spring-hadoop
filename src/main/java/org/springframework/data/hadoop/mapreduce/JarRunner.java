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
package org.springframework.data.hadoop.mapreduce;

import org.apache.hadoop.util.Tool;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Factory bean for executing Hadoop jars. Tries a best-effort in passing the configuration to the jar and preventing malicious behaviour (such as System.exit()).
 * See the reference documentation for more details.
 * Using the {@link Tool} interface is highly recommended in all cases.
 * 
 * <p/>Note by default, the runner is configured to execute at startup. One can customize this behaviour through {@link #setRunAtStartup(boolean)}/
 * <p/>This class is a factory bean - if {@link #setRunAtStartup(boolean)} is set to false, then the action (namely the execution of the Tool) is postponed by the call
 * to {@link #getObject()}.
 * 
 * @author Costin Leau
 */
public class JarRunner extends JarExecutor implements FactoryBean<Integer>, InitializingBean {

	private volatile Integer result = null;
	private boolean runAtStartup = false;

	@Override
	public Integer getObject() throws Exception {
		if (result == null) {
			result = runCode();
		}
		return result;
	}

	@Override
	public Class<?> getObjectType() {
		return int.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();

		if (runAtStartup) {
			getObject();
		}
	}

	/**
	 * Indicates whether the jar should run at container startup (the default) or not.
	 *
	 * @param runAtStartup The runAtStartup to set.
	 */
	public void setRunAtStartup(boolean runAtStartup) {
		this.runAtStartup = runAtStartup;
	}
}