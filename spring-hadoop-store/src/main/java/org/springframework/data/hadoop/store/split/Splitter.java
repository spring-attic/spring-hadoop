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
package org.springframework.data.hadoop.store.split;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;

/**
 * A strategy interface for creating a list of {@link Split}s based on a
 * {@link Path} representing a hdfs resource.
 *
 * @author Janne Valkealahti
 *
 */
public interface Splitter {

	/**
	 * Gets the input splits for a {@link Path}. A path needs to
	 * be a resource which can be split into a list of splits. Actual
	 * implementation will define if split is enforced to be a single
	 * file or a collection of files.
	 *
	 * @param path the path
	 * @return the input splits
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	List<Split> getSplits(Path path) throws IOException;

}