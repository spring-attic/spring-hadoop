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
package org.springframework.data.hadoop.store.strategy.naming;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.springframework.util.StringUtils;

/**
 * A {@code FileNamingStrategy} which simply uses a rolling counter to give unique file name.
 *
 * @author Janne Valkealahti
 *
 */
public class RollingFileNamingStrategy extends AbstractFileNamingStrategy {

	private final static Log log = LogFactory.getLog(RollingFileNamingStrategy.class);

	private volatile int counter = 0;

	private volatile String prefix = "-";

	@Override
	public Path resolve(Path path) {
		if (!isEnabled()) {
			return path;
		} else if (path != null) {
			return new Path(path.getParent(), path.getName() + prefix + Integer.toString(counter));
		} else {
			return new Path(Integer.toString(counter));
		}
	}

	@Override
	public void next() {
		counter++;
	}

	@Override
	public void reset() {
		counter = 0;
	}

	@Override
	public Path init(Path path) {
		path = super.init(path);
		log.debug("Initialising from path=" + path);
		if (path != null) {
			String name = path.getName();

			// find numeric part
			Pattern counterPattern = Pattern.compile(prefix + "(" + "\\d+" + ")");
			Matcher m = counterPattern.matcher(name);
			while (m.find()) {
				try {
					counter = Integer.parseInt(m.group(1)) + 1;
				} catch (NumberFormatException e) {
					// we don't care about numeric parts we can't parse
				}
			}
			log.debug("Initialized counter starting from " + counter);

			// find complete part handled by this strategy
			Pattern replacePattern = Pattern.compile("(" + prefix + "\\d+" + ")(.*)");
			m = replacePattern.matcher(name);

			// remove a rolling part
			name = m.replaceFirst("$2");
			if (StringUtils.hasText(name)) {
				path = new Path(path.getParent(), name);
				log.debug("Removed handled prefix, path is now " + path);
			} else {
				path = null;
				log.debug("Removed last handled name part, returning null");
			}
		}
		return path;
	}

	/**
	 * Sets the prefix preceding rolling number part.
	 *
	 * @param prefix the new prefix
	 */
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	@Override
	public RollingFileNamingStrategy createInstance() {
		RollingFileNamingStrategy instance = new RollingFileNamingStrategy();
		instance.setCodecInfo(getCodecInfo());
		instance.setOrder(getOrder());
		instance.setEnabled(isEnabled());
		instance.setPrefix(prefix);
		return instance;
	}

}
