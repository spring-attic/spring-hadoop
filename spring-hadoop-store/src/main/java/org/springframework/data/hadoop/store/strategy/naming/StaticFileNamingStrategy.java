/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.store.strategy.naming;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * A {@code FileNamingStrategy} which simply uses a static file name.
 *
 * @author Janne Valkealahti
 *
 */
public class StaticFileNamingStrategy extends AbstractFileNamingStrategy {

	private final static Log log = LogFactory.getLog(StaticFileNamingStrategy.class);

	private final static String DEFAULT_FILENAME = "data";

	private String fileName;

	/**
	 * Instantiates a new static file naming strategy.
	 */
	public StaticFileNamingStrategy() {
		this(DEFAULT_FILENAME);
	}

	/**
	 * Instantiates a new static file naming strategy.
	 *
	 * @param fileName the file name
	 */
	public StaticFileNamingStrategy(String fileName) {
		Assert.hasText(fileName, "Filename cannot be empty");
		this.fileName = fileName;
	}

	@Override
	public Path resolve(Path path) {
		if (path != null) {
			return new Path(path, fileName);
		}
		else {
			return new Path(fileName);
		}
	}

	@Override
	public void reset() {
		// we're static, nothing to do
	}

	@Override
	public Path init(Path path) {
		path = super.init(path);
		log.debug("Initialising from path=" + path);
		if (path != null) {
			if (path.getName().startsWith(fileName)) {

				String name = path.getName().substring(fileName.length());
				if (StringUtils.hasText(name)) {
					path = new Path(path.getParent(), name);
					log.debug("Removed handled prefix, path is now " + path);
				} else {
					path = null;
					log.debug("Removed last handled name part, returning null");
				}
			}
		}
		return path;
	}

	/**
	 * Sets the file name.
	 *
	 * @param fileName the new file name
	 */
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

}
