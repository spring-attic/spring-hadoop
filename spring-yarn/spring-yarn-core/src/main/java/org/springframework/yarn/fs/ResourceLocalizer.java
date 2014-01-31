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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;

/**
 * Interface for resource localizer implementation. This loosely
 * follows requirements for Yarn's file distribution of
 * {@link LocalResource} instances.
 *
 * @author Janne Valkealahti
 *
 */
public interface ResourceLocalizer {
	// TODO: this api is ambiguous and not clean to use for distribute vs copy vs resolve
	/**
	 * Gets a map of {@link LocalResource} instances. Underlying
	 * instances of {@link LocalResource}s needs to be fully
	 * initialised including resource size and timestamp.
	 *
	 * @return The map containing {@link LocalResource} instances
	 */
	Map<String, LocalResource> getResources();

	/**
	 * If underlying implementation needs to do operations
	 * on hdfs filesystem or any other preparation work,
	 * calling of this method should make implementation
	 * ready to return resources from {@link #getResources()}
	 * command.
	 * <p>
	 * Effectively result of calling this method should be same
	 * as calling both {@linkplain #copy()} and {@linkplain #resolve()}
	 * methods manually.
	 */
	void distribute();

	/**
	 * Only copy files into hdfs.
	 */
	void copy();

	/**
	 * Only resolve localized resources.
	 */
	void resolve();

	/**
	 * Sets the staging directory. If not set, path is
	 * determined by the implementation. Default value
	 * can i.e. be user's hadoop home directory.
	 *
	 * @param stagingDirectory the new staging directory
	 */
	void setStagingDirectory(Path stagingDirectory);

	/**
	 * Sets the staging id. Id is used together with {@link Path}
	 * set in {@link #setStagingDirectory(Path)} to post fix unique
	 * runtime staging path. If not set simultaneous instances of
	 * same application may override files.
	 *
	 * @param stagingId the new staging id
	 */
	void setStagingId(String stagingId);

	/**
	 * Cleans all leftovers what has been created during
	 * the distribute process. These can i.e. be files
	 * in staging directory.
	 *
	 * @return true, if successful
	 */
	boolean clean();

}
