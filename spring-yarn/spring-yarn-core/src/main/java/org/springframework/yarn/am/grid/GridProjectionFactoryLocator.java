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
package org.springframework.yarn.am.grid;

import java.util.Set;

/**
 * A locator for {@link GridProjectionFactory} instances.
 *
 * @author Janne Valkealahti
 *
 */
public interface GridProjectionFactoryLocator {

	/**
	 * Locates a {@link GridProjectionFactory}.
	 *
	 * @param projectionType the projection type
	 * @return the grid projection factory
	 */
	GridProjectionFactory getGridProjectionFactory(String projectionType);

	/**
	 * Gets a registered projection types known to this locator.
	 *
	 * @return the registered projection types
	 */
	Set<String> getRegisteredProjectionTypes();

}
