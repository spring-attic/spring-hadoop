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
package org.springframework.data.hadoop.store;

import java.io.IOException;

/**
 * A {@code DataReader} is an interface for reader implementation
 * able to read entities from a store.
 *
 * @author Janne Valkealahti
 *
 * @param <T> Type of an entity for the reader
 */
public interface DataReader<T> {

	/**
	 * Read next entity from a reader.
	 *
	 * @return the entity or <code>null</code>
	 * @throws IOException if an I/O error occurs
	 */
	T read() throws IOException;

}
