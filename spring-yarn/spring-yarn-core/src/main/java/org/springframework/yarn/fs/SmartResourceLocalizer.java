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

import org.apache.hadoop.fs.Path;

/**
 * {@code SmartResourceLocalizer} provides additional functionality on
 * top of {@link ResourceLocalizer} order to handle more fine grained
 * handling of resource localizing.
 * <p>
 * These smart functionalities includes of controlling localizing process
 * manually, copy files into file system prior the localization and
 * use staging concept of application files.
 * <p>
 * Logic of having methods for manually resolving, distributing and
 * copying resources is to have an early possibility to fail fast
 * if something goes wrong. Although one need to remember that on
 * all circumstances calling {@link ResourceLocalizer#getResources()}
 * should always be enough.
 *
 * @author Janne Valkealahti
 * @see ResourceLocalizer
 */
public interface SmartResourceLocalizer extends ResourceLocalizer {

	/**
	 * If underlying implementation needs to do operations
	 * on file system or any other preparation work,
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
	 * Manually resolve resources before distribution process
	 * is initiated. Using this method should always be optional
	 * and executed before distribution if it has not already
	 * been done.
	 */
	void resolve();

	/**
	 * If resource localizer has knowledge to copy files
	 * into file system, calling this method effectively
	 * should do all tasks for that specific functionality.
	 */
	void copy();

	/**
	 * Cleans all leftovers what has been created during
	 * the distribute process. These can i.e. be files
	 * in staging directory.
	 *
	 * @return true, if cleanup were attempted and was successful
	 */
	boolean clean();

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

}
