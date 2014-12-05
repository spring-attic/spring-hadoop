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
package org.springframework.yarn;

import java.io.IOException;

import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.springframework.dao.UncategorizedDataAccessException;

/**
 * General exception indicating a problem in components interacting with yarn.
 * Main point of wrapping native yarn exceptions inside this is to have common
 * Spring dao exception hierarchy.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnSystemException extends UncategorizedDataAccessException {

	private static final long serialVersionUID = -280113474245028099L;

	/**
	 * Constructs YarnSystemException from {@link YarnException}.
	 *
	 * @param e the {@link YarnException}
	 */
	public YarnSystemException(YarnException e) {
		super(e.getMessage(), e);
	}

	/**
	 * Constructs YarnSystemException from {@link YarnRuntimeException}.
	 *
	 * @param e the {@link YarnRuntimeException}
	 */
	public YarnSystemException(YarnRuntimeException e) {
		super(e.getMessage(), e);
	}

	/**
	 * Constructs YarnSystemException from {@link RemoteException}.
	 *
	 * @param e the {@link RemoteException}
	 */
	public YarnSystemException(IOException e) {
		super(e.getMessage(), e);
	}

	/**
	 * Constructs a general YarnSystemException.
	 *
	 * @param message the message
	 * @param e the exception
	 */
	public YarnSystemException(String message, Exception e) {
		super(message, e);
	}

	/**
	 * Constructs a general YarnSystemException.
	 *
	 * @param message the message
	 * @param cause the throwable cause
	 */
	public YarnSystemException(String message, Throwable cause) {
		super(message, cause);
	}

}
