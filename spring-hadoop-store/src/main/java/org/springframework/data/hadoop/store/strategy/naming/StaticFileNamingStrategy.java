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
import org.springframework.util.StringUtils;

/**
 * A {@code FileNamingStrategy} which simply uses a static file name.
 *
 * @author Janne Valkealahti
 *
 */
public class StaticFileNamingStrategy extends AbstractFileNamingStrategy {

	private final static Log log = LogFactory.getLog(StaticFileNamingStrategy.class);

	private final static String DEFAULT_NAME = "data";

	private String name;

	private String prefix;

	/**
	 * Instantiates a new static file naming strategy.
	 */
	public StaticFileNamingStrategy() {
		this(DEFAULT_NAME);
	}

	/**
	 * Instantiates a new static file naming strategy.
	 *
	 * @param name the name
	 */
	public StaticFileNamingStrategy(String name) {
		this.name = name;
	}

	public StaticFileNamingStrategy(String name, String prefix) {
		this.name = name;
		this.prefix = prefix;
	}

	@Override
	public Path resolve(Path path) {
		String part = getNamingPart();
		if (!isEnabled() || !StringUtils.hasText(part)) {
			return path;
		}
		if (path != null) {
			return new Path(path.getParent(), path.getName() + part);
		}
		else {
			return new Path(part);
		}
	}

	@Override
	public void next() {
		// we're static, nothing to do
	}

	@Override
	public Path init(Path path) {
		path = super.init(path);
		log.debug("Init using path=[" + path + "]");
		if (path != null) {
			String part = getNamingPart();
			if (path.getName().startsWith(part)) {

				String name = path.getName().substring(part.length());
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

	@Override
	public StaticFileNamingStrategy createInstance() {
		StaticFileNamingStrategy instance = new StaticFileNamingStrategy();
		instance.setCodecInfo(getCodecInfo());
		instance.setOrder(getOrder());
		instance.setName(name);
		instance.setPrefix(prefix);
		instance.setEnabled(isEnabled());
		return instance;
	}

	/**
	 * Sets the file name part.
	 *
	 * @param name the new name part
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Sets the prefix preceding name part.
	 *
	 * @param prefix the new prefix
	 */
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	private String getNamingPart() {
		return (StringUtils.hasText(prefix) ? prefix : "") + (StringUtils.hasText(name) ? name : "");
	}

}
