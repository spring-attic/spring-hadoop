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

import org.springframework.dao.NonTransientDataAccessException;

/**
 * An exception that indicates there was an error accessing the the underlying
 * HDFS store implementation.
 *
 * @author Janne Valkealahti
 * @author Thomas Risberg
 */
public class StoreException extends NonTransientDataAccessException {

	private static final long serialVersionUID = 7066482966443178810L;

	/**
	 * Constructor for StoreException.
	 * @param msg message
	 * @param cause the root cause
	 */
	public StoreException(String msg, Throwable cause) {
		super(msg, cause);
	}

	/**
	 * Constructor for StoreException.
	 * @param msg message
	 */
	public StoreException(String msg) {
		super(msg);
	}

}
