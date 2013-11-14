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
package org.springframework.yarn.am;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

/**
 * Interface for interceptors that are able to view and/or modify the
 * {@link ContainerLaunchContext} before the container is launched.
 *
 * @author Janne Valkealahti
 *
 */
public interface ContainerLauncherInterceptor {

	/**
	 * Invoked before the {@link ContainerLaunchContext} is used
	 * to launch the container.
	 *
	 * @param context the {@link ContainerLaunchContext}
	 * @param container the {@link Container}
	 * @return Unchanged or modified {@link ContainerLaunchContext}
	 */
	ContainerLaunchContext preLaunch(Container container, ContainerLaunchContext context);

}
