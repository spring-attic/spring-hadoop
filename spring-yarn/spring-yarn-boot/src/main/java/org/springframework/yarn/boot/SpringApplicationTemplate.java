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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * {@link SpringApplicationTemplate} is meant to safely run {@link SpringApplication}
 * from a {@link SpringApplicationBuilder} with a callback action {@link SpringApplicationCallback}
 * to do operations against an {@link ApplicationContext} and then optionally return an value
 * back to a caller.
 *
 * @author Janne Valkealahti
 *
 */
public class SpringApplicationTemplate {

	private SpringApplicationBuilder builder;

	public SpringApplicationTemplate(SpringApplicationBuilder builder) {
		this.builder = builder;
	}

	// TODO: when we get better internal support for executing
	//       beans via reflection aka, container activator for pojos,
	//       add support methods here not to have need to always use raw execute

	/**
	 * Execute spring application from a builder. This method will automatically
	 * close a context associated with a spring application.
	 *
	 * @param action the action callback
	 * @param args the boot application args
	 * @return the value from an execution
	 */
	public <T> T execute(SpringApplicationCallback<T> action, String... args) {
		ConfigurableApplicationContext context = null;
		try {
			context = builder.run(args);
			return action.runWithSpringApplication(context);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (context != null) {
				try {
					context.close();
				}
				catch (Exception e) {
				}
				context = null;
			}
		}
	}

}
