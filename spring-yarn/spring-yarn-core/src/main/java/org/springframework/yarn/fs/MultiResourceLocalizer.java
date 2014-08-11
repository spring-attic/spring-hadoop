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
package org.springframework.yarn.fs;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.LocalResource;

/**
 * Extension of {@link ResourceLocalizer} adding functionality to
 * use multiple {@link ResourceLocalizer}s.
 *
 * @author Janne Valkealahti
 *
 */
public interface MultiResourceLocalizer extends ResourceLocalizer {

	/**
	 * Gets a map of {@link LocalResource} instances identified by
	 * an arbitrary id. Passing <code>null</code> or unknown id
	 * will fall back to base method {@link ResourceLocalizer#getResources()}.
	 *
	 * @param id identifier for local resources
	 * @return The map containing {@link LocalResource} instances
	 * @see ResourceLocalizer#getResources()
	 */
	Map<String, LocalResource> getResources(String id);

}
