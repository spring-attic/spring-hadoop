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

import org.apache.hadoop.yarn.api.records.LocalResourceType;

/**
 * {@code LocalResourcesSelector} builds and selects a list of
 * {@link Entry}s having information about a path and
 * a {@link LocalResourceType} of that path.
 * <p>
 * This interface is supposed to ease a configuration is cases
 * where it is too difficult to hard code possible values or
 * of the logic how these values are chosen needs to be able
 * to altered.
 * <p>
 * Interface itself doesn't care how entries are selected, thus
 * implementation may choose to work in a static way or actually
 * checking something from a file system.
 *
 * @author Janne Valkealahti
 *
 */
public interface LocalResourcesSelector {

	/**
	 * Select a {@link List} of {@link Entry}s.
	 * This method cannot not return <code>NULL</code>.
	 *
	 * @param dir the base directory
	 * @return the list of entries
	 */
	List<Entry> select(String dir);

	/**
	 * Entrys used by this interface.
	 */
	public class Entry {

		private final String path;
		private final LocalResourceType type;

		/**
		 * Instantiates a new entry.
		 *
		 * @param path the path
		 * @param type the type
		 */
		public Entry(String path, LocalResourceType type) {
			super();
			this.path = path;
			this.type = type;
		}

		/**
		 * Gets the entry path.
		 *
		 * @return the path
		 */
		public String getPath() {
			return path;
		}

		/**
		 * Gets the entry {@link LocalResourceType}.
		 *
		 * @return the local resource type
		 */
		public LocalResourceType getType() {
			return type;
		}
	}

}
