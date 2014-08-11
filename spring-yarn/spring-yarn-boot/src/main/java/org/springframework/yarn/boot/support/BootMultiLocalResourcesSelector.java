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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.yarn.fs.LocalResourcesSelector;
import org.springframework.yarn.fs.MultiLocalResourcesSelector;

/**
 * Default implementation of a {@link MultiLocalResourcesSelector}.
 *
 * @author Janne Valkealahti
 *
 */
public class BootMultiLocalResourcesSelector implements MultiLocalResourcesSelector {

	private final HashMap<String, LocalResourcesSelector> selectors = new HashMap<String, LocalResourcesSelector>();

	/**
	 * Instantiates a new boot multi local resources selector.
	 *
	 * @param selector the default selector
	 * @param selectors the custom selectors
	 */
	public BootMultiLocalResourcesSelector(LocalResourcesSelector selector, Map<String, LocalResourcesSelector> selectors) {
		this.selectors.put(null, selector);
		this.selectors.putAll(selectors);
	}

	@Override
	public List<Entry> select(String dir) {
		return selectors.get(null).select(dir);
	}

	@Override
	public List<Entry> select(String id, String dir) {
		LocalResourcesSelector selector = selectors.get(id);
		if (selector != null) {
			return selector.select(dir);
		} else {
			return selectors.get(null).select(dir);
		}
	}

}
