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

/**
 * A simple interface for container implementations
 * which should work together with rest of the framework
 * when something in a container should be executed.
 *
 * @author Janne Valkealahti
 *
 */
public interface YarnContainer {

	/**
	 * This method is called when something is ran
	 * in a container.
	 */
	void run();

	/**
	 * Sets the environment variables. This method should be
	 * used by a launcher or any other party handling
	 * creation of a container.
	 *
	 * @param environment the environment variables
	 */
	public void setEnvironment(Map<String, String> environment);

	/**
	 * Sets the parameters. This method should be
	 * used by a launcher or any other party handling
	 * being aware of a command line parameters.
	 *
	 * @param parameters the parameters
	 */
	public void setParameters(Properties parameters);

}
