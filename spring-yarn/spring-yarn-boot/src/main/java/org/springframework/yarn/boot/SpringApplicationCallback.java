/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.yarn.boot;

import org.springframework.context.ApplicationContext;

/**
 * An action to take on a Spring Boot Application.
 *
 * @author Janne Valkealahti
 *
 * @param <T> The return type from an action
 */
public interface SpringApplicationCallback<T> {

	/**
	 * Perform an actions with a given {@link ApplicationContext}.
	 *
	 * @param context the application context
	 * @return the return value of type T
	 * @throws Exception the exception if error occurred
	 */
	T runWithSpringApplication(ApplicationContext context) throws Exception ;

}
