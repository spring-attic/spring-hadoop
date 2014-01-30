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
package org.springframework.yarn.batch.repository;

import org.springframework.yarn.integration.ip.mind.binding.BaseObject;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

/**
 * Interface for interceptors that are able to view and/or modify the
 * messages when communication is handled with a service operating
 * with {@link BaseObject} and {@link BaseResponseObject}.
 *
 * @author Janne Valkealahti
 *
 */
public interface JobRepositoryRemoteServiceInterceptor {

	/**
	 * Invoked after request is made using {@link BaseObject}.
	 * This allows modification of a request. If this method
	 * returns <code>null</code>, the interceptor chain will
	 * break indicating that one of the interceptor should handle
	 * the request in {@link #handleRequest(BaseObject)}
	 * method.
	 *
	 * @param baseObject the request base object
	 * @return the base object
	 */
	BaseObject preRequest(BaseObject baseObject);

	/**
	 * Handles request if at least one interceptor returned <code>null</code>
	 * from a {@link #preRequest(BaseObject)} method.
	 *
	 * @param baseObject the base object
	 * @return the base response object
	 */
	BaseResponseObject handleRequest(BaseObject baseObject);

	/**
	 * Invoked after request is made using {@link BaseObject}
	 * and before {@link BaseResponseObject} is sent to further
	 * processing. This allows modification of a response.
	 *
	 * @param baseResponseObject the response object
	 * @return the base response object
	 */
	BaseResponseObject postRequest(BaseResponseObject baseResponseObject);

}
