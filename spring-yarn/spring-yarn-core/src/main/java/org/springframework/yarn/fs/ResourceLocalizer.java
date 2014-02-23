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
package org.springframework.yarn.fs;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.LocalResource;

/**
 * {@code ResourceLocalizer} is a simple interface used during
 * a container launch to determine a localized resources. This
 * loosely follows requirements for YARN's file distribution of
 * {@link LocalResource} instances.
 *
 * @author Janne Valkealahti
 * @see SmartResourceLocalizer
 */
public interface ResourceLocalizer {

	/**
	 * Gets a map of {@link LocalResource} instances. Underlying
	 * instances of {@link LocalResource}s needs to be fully
	 * initialised including resource size and timestamp.
	 * <p>
	 * Calling this method should do all needed preparation work
	 * needed to return {@link LocalResource}s.
	 *
	 * @return The map containing {@link LocalResource} instances
	 */
	Map<String, LocalResource> getResources();

}
