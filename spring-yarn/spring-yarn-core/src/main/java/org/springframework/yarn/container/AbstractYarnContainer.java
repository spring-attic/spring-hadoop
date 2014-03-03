/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.yarn.container;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.yarn.listener.CompositeContainerStateListener;
import org.springframework.yarn.listener.ContainerStateListener;
import org.springframework.yarn.listener.ContainerStateListener.ContainerState;

/**
 * Base implementation of {@link YarnContainer} providing
 * some common functionality like environment properties,
 * command line parameters and handling of the {@link #run()}.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractYarnContainer implements LongRunningYarnContainer, YarnContainerRuntime {

	/** Environment variables for the process. */
	private Map<String, String> environment;

	/** Parameters passed to the container. */
	private Properties parameters;

	/** Listener handling state events */
	private CompositeContainerStateListener stateListener = new CompositeContainerStateListener();

	/** Yarn configuration */
	private Configuration configuration;

	@Override
	public final void run() {
		runInternal();
	}

	@Override
	public void setEnvironment(Map<String, String> environment) {
		this.environment = environment;
	}

	@Override
	public void setParameters(Properties parameters) {
		this.parameters = parameters;
	}

	/**
	 * Gets the Yarn configuration.
	 *
	 * @return the Yarn configuration
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Sets the Yarn configuration.
	 *
	 * @param configuration the new Yarn configuration
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Gets the environment variable.
	 *
	 * @param key the key
	 * @return the environment variable or {@code null} if key doesn't exist
	 */
	public String getEnvironment(String key) {
		return environment != null ? environment.get(key) : null;
	}

	/**
	 * Gets the environment.
	 *
	 * @return the environment
	 */
	public Map<String, String> getEnvironment() {
		return environment;
	}

	/**
	 * Gets the parameters.
	 *
	 * @return the parameters
	 */
	public Properties getParameters() {
		return parameters;
	}

	@Override
	public void addContainerStateListener(ContainerStateListener listener) {
		stateListener.register(listener);
	}

	@Override
	public boolean isWaitCompleteState() {
		return false;
	}

	/**
	 * Notify completed state to container state listeners.
	 */
	protected void notifyCompleted() {
		stateListener.state(ContainerState.COMPLETED);
	}

	/**
	 * Internal method to handle the actual
	 * {@link #run()} method.
	 */
	protected abstract void runInternal();

}
