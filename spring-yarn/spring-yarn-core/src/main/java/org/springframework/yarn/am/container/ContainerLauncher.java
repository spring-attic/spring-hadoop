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
package org.springframework.yarn.am.container;

import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;

/**
 * General interface for container launcher.
 *
 * @author Janne Valkealahti
 *
 */
public interface ContainerLauncher {

	/**
	 * Launch container {@link Container} using given list
	 * of commands.
	 *
	 * @param container the {@link Container}
	 * @param commands the list of commands
	 */
	void launchContainer(Container container, List<String> commands);

}
