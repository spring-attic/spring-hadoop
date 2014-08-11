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

import org.springframework.core.NestedRuntimeException;

/**
 * Base class for exceptions thrown by {@link YarnContainerClusterApplication}
 * whenever it encounters client-side errors.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnContainerClusterClientException extends NestedRuntimeException {

	private static final long serialVersionUID = 849031924295533758L;

	/**
	 * Construct a new instance of {@code YarnContainerClusterClientException}
	 * with the given message.
	 *
	 * @param msg the message
	 */
	public YarnContainerClusterClientException(String msg) {
		super(msg);
	}

	/**
	 * Construct a new instance of {@code YarnContainerClusterClientException}
	 * with the given message and exception.
	 *
	 * @param msg the message
	 * @param cause the exception
	 */
	public YarnContainerClusterClientException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
