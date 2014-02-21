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
package org.springframework.yarn.boot.support;

import org.springframework.yarn.fs.AbstractLocalResourcesSelector;
import org.springframework.yarn.fs.LocalResourcesSelector;

/**
 * Boot specific default {@link LocalResourcesSelector} implementation.
 *
 * @author Janne Valkealahti
 *
 */
public class BootLocalResourcesSelector extends AbstractLocalResourcesSelector {

	/**
	 * Instantiates a new boot local resources selector.
	 */
	public BootLocalResourcesSelector() {
		this(null);
	}

	/**
	 * Instantiates a new boot local resources selector.
	 *
	 * @param mode the mode
	 */
	public BootLocalResourcesSelector(Mode mode) {
		if (Mode.APPMASTER.equals(mode)) {
			addPatterns("*appmaster*jar", "*appmaster*zip");
		} else if (Mode.CONTAINER.equals(mode)) {
			addPatterns("*container*jar", "*container*zip");
		}
	}

	/**
	 * The Enum indicating working mode for {@link BootLocalResourcesSelector}.
	 */
	public enum Mode {

		/** Mode indicating selector is choosing files for appmaster. */
		APPMASTER,

		/** Mode indicating selector is choosing files for container. */
		CONTAINER
	}

}
