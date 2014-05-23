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
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.util.StringUtils;

/**
 * A {@code FileNamingStrategy} which adds suffix based on known codec.
 *
 * @author Janne Valkealahti
 *
 */
public class CodecFileNamingStrategy extends AbstractFileNamingStrategy {

	private final static Log log = LogFactory.getLog(CodecFileNamingStrategy.class);

	/**
	 * Instantiates a new codec file naming strategy.
	 */
	public CodecFileNamingStrategy() {
	}

	@Override
	public int getOrder() {
		return LOWEST_PRECEDENCE - 1;
	}

	@Override
	public Path resolve(Path path) {
		if (!isEnabled()) {
			return path;
		}
		CodecInfo c = getCodecInfo();
		String suffix = c != null ? "." + c.getDefaultSuffix() : "";
		if (path != null) {
			return path.suffix(suffix);
		} else if (StringUtils.hasText(suffix)){
			return new Path(suffix);
		} else {
			return path;
		}
	}

	@Override
	public void next() {
	}

	@Override
	public Path init(Path path) {
		path = super.init(path);
		log.debug("Initialising from path=" + path);

		CodecInfo c = getCodecInfo();
		String suffix = c != null ? "." + c.getDefaultSuffix() : "";

		if (path != null && StringUtils.hasText(suffix)) {
			if (path.getName().startsWith(suffix)) {
				String name = path.getName().substring(suffix.length());
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
	public CodecFileNamingStrategy createInstance() {
		CodecFileNamingStrategy instance = new CodecFileNamingStrategy();
		instance.setCodecInfo(getCodecInfo());
		instance.setOrder(getOrder());
		instance.setEnabled(isEnabled());
		return instance;
	}

}
