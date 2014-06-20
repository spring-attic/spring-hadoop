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
package org.springframework.data.hadoop.store.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.RolloverStrategy;

/**
 * Context strategy keeping output state.
 *
 * @author Janne Valkealahti
 *
 */
public class OutputContext {

	private final static Log log = LogFactory.getLog(OutputContext.class);

	private FileNamingStrategy fileNamingStrategy;

	private RolloverStrategy rolloverStrategy;

	/**
	 * Instantiates a new strategy context.
	 */
	public OutputContext() {
	}

	/**
	 * Sets the write position.
	 *
	 * @param position the new write position
	 */
	public void setWritePosition(long position) {
		if (rolloverStrategy != null) {
			rolloverStrategy.setWritePosition(position);
		}
	}

	/**
	 * Gets the rollover state.
	 *
	 * @return the rollover state
	 */
	public boolean getRolloverState() {
		if (rolloverStrategy != null) {
			return rolloverStrategy.hasRolled();
		} else {
			return false;
		}
	}

	/**
	 * Roll strategies.
	 */
	public void rollStrategies() {
		if (rolloverStrategy != null) {
			rolloverStrategy.reset();
		}
		if (fileNamingStrategy != null) {
			fileNamingStrategy.next();
		}
	}

	/**
	 * Resolve path.
	 *
	 * @param path the path
	 * @return the path
	 */
	public Path resolvePath(Path path) {
		// start by passing null indicating we're starting with
		// empty path. paths are then appended and we combine
		// returned path with base path given to this method.
		Path p = fileNamingStrategy != null ? fileNamingStrategy.resolve(null) : null;
		return p != null ? new Path(path, p) : path;
	}

	/**
	 * Inits the context from a {@link Path}
	 *
	 * @param path the path
	 * @return the path
	 */
	public Path init(Path path) {
		log.info("Init from path=" + path);
		if (fileNamingStrategy != null) {
			Path p = fileNamingStrategy.init(path);
			if (p != null) {
				fileNamingStrategy.reset();
			}
			return p;
		}
		return null;
	}

	/**
	 * Sets the codec info.
	 *
	 * @param codecInfo the new codec info
	 */
	public void setCodecInfo(CodecInfo codecInfo) {
		log.info("Setting codecInfo=" + codecInfo);
		if (fileNamingStrategy != null) {
			fileNamingStrategy.setCodecInfo(codecInfo);
		}
	}

	/**
	 * Sets the file naming strategy.
	 *
	 * @param fileNamingStrategy the new file naming strategy
	 */
	public void setFileNamingStrategy(FileNamingStrategy fileNamingStrategy) {
		log.info("Setting fileNamingStrategy=" + fileNamingStrategy);
		this.fileNamingStrategy = fileNamingStrategy;
	}

	/**
	 * Sets the rollover strategy.
	 *
	 * @param rolloverStrategy the new rollover strategy
	 */
	public void setRolloverStrategy(RolloverStrategy rolloverStrategy) {
		log.info("Setting rolloverStrategy=" + rolloverStrategy);
		this.rolloverStrategy = rolloverStrategy;
	}

}
