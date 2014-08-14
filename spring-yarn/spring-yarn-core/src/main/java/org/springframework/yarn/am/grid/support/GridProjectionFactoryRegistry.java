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
package org.springframework.yarn.am.grid.support;

import java.util.HashSet;
import java.util.Set;

import org.springframework.yarn.am.grid.GridProjectionFactory;
import org.springframework.yarn.am.grid.GridProjectionFactoryLocator;

/**
 * Default implementation of a {@link GridProjectionFactoryLocator}.
 *
 * @author Janne Valkealahti
 *
 */
public class GridProjectionFactoryRegistry implements GridProjectionFactoryLocator {

	private final Set<GridProjectionFactory> factories = new HashSet<GridProjectionFactory>();

	@Override
	public GridProjectionFactory getGridProjectionFactory(String projectionType) {
		for (GridProjectionFactory factory : factories) {
			if(factory.getRegisteredProjectionTypes().contains(projectionType)) {
				return factory;
			}
		}
		return null;
	}

	@Override
	public Set<String> getRegisteredProjectionTypes() {
		HashSet<String> ids = new HashSet<String>();
		for (GridProjectionFactory factory : factories) {
			ids.addAll(factory.getRegisteredProjectionTypes());
		}
		return ids;
	}

	public void addGridProjectionFactory(GridProjectionFactory factory) {
		factories.add(factory);
	}

}
