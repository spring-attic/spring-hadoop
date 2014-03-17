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

import org.springframework.dao.UncategorizedDataAccessException;

/**
 * Generic exception thrown from a
 * {@link SpringApplicationTemplate#execute(SpringApplicationCallback, String...)}
 * to be able to wrap a possible exception.
 *
 * @author Janne Valkealahti
 *
 */
public class SpringApplicationException extends UncategorizedDataAccessException {

	private static final long serialVersionUID = 6791172815454960337L;

	/**
	 * Instantiates a new spring application exception.
	 *
	 * @param msg the message
	 * @param cause the cause
	 */
	public SpringApplicationException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
