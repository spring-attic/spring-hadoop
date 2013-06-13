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
package org.springframework.yarn.launch;

/**
 * Implementation of the {@link SystemExiter} interface that calls the standards
 * System.exit method. It should be noted that there will be no unit tests for
 * this class, since there is only one line of actual code, that would only be
 * testable by mocking System or Runtime.
 *
 * @author Janne Valkealahti
 *
 */
public class JvmSystemExiter implements SystemExiter {

	/**
	 * Delegate call to System.exit() with the argument provided. This should only
	 * be used in a scenario where a particular status needs to be returned to
	 * an external scheduler.
	 *
	 * @see org.springframework.yarn.launch.SystemExiter#exit(int)
	 */
	@Override
	public void exit(int status) {
		System.exit(status);
	}

}
