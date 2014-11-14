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
package org.springframework.yarn.boot.app;

/**
 * Interface for running client applications.
 *
 * @author Janne Valkealahti
 *
 * @param <R> the type of output
 */
public interface ClientApplicationRunner<R> {

	/**
	 * Run the application.
	 *
	 * @return the output
	 */
	R run();

	/**
	 * Run the application.
	 *
	 * @param args the application arguments
	 * @return the output
	 */
	R run(String... args);

}
