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
package org.springframework.yarn.am;

import java.util.Map;
import java.util.Properties;

import org.springframework.yarn.listener.AppmasterStateListener;

/**
 * Interface defining main application master methods
 * needed for external launch implementations.
 *
 * @author Janne Valkealahti
 *
 */
public interface YarnAppmaster {

	/**
	 * Submit and run application.
	 */
	void submitApplication();

	/**
	 * Sets the environment variables. This method should be
	 * used by a launcher or any other party handling
	 * creation of an appmaster.
	 *
	 * @param environment the environment variables
	 */
	public void setEnvironment(Map<String, String> environment);

	/**
	 * Sets parameters for the appmaster.
	 *
	 * @param parameters the parameters to set
	 */
	void setParameters(Properties parameters);


	/**
	 * Adds the appmaster state listener.
	 *
	 * @param listener the {@link AppmasterStateListener}
	 */
	void addAppmasterStateListener(AppmasterStateListener listener);

}
