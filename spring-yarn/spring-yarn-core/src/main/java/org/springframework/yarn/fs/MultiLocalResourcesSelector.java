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

import java.util.List;

import org.springframework.yarn.fs.LocalResourcesSelector.Entry;

/**
 * Extension of {@link LocalResourcesSelector} adding functionality to
 * use multiple {@link LocalResourcesSelector}s.
 *
 * @author Janne Valkealahti
 *
 */
public interface MultiLocalResourcesSelector extends LocalResourcesSelector {

	/**
	 * Select a {@link List} of {@link Entry}s identified by
	 * an arbitrary id.
	 *
	 * @param id the identifier
	 * @param dir the base directory
	 * @return the list of entries
	 * @see LocalResourcesSelector#select(String)
	 */
	List<Entry> select(String id, String dir);

}
