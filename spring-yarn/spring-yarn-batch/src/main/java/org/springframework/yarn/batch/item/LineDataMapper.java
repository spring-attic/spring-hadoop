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
package org.springframework.yarn.batch.item;

/**
 * Interface for mapping lines (strings) to domain objects. Implementations of
 * this interface perform the actual work of parsing a line without having to
 * deal with how the line was obtained.
 *
 * @param <T> the type of the domain object
 */
public interface LineDataMapper<T> {

	/**
	 * Implementations must implement this method to map the
	 * provided line to the parameter type T.
	 *
	 * @param line to be mapped
	 * @return mapped object of type T
	 * @throws Exception if error occured while parsing.
	 */
	T mapLine(String line) throws Exception;

}
