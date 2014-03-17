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
package org.springframework.yarn.client;

import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * A {@code ApplicationYarnClient} is an extension of {@link YarnClient}
 * introducing more direct semantics of an application. Core yarn and generally
 * when something is executed on yarn, there are no hard dependencies of
 * existing application files.
 * <p>
 * This interface adds these semantics by using an {@link ApplicationDescriptor}
 * which an implementation can for example use to guard against various
 * application problems like not overwriting existing application instance or
 * trying to launch an application which doesn't exist.
 *
 * @author Janne Valkealahti
 *
 */
public interface ApplicationYarnClient extends YarnClient {

	/**
	 * Install application based on {@link ApplicationDescriptor}.
	 *
	 * @param descriptor the application descriptor
	 */
	void installApplication(ApplicationDescriptor descriptor);

	/**
	 * Submit application based on {@link ApplicationDescriptor}.
	 *
	 * @param descriptor the application descriptor
	 * @return the application id
	 */
	ApplicationId submitApplication(ApplicationDescriptor descriptor);

}
