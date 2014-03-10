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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.split.Split;

/**
 * Base class for input readers sharing common functionality
 * for {@link Split} and {@link InputContext}.
 *
 * @author Janne Valkealahti
 *
 */
public class InputStoreObjectSupport extends StoreObjectSupport {

	private final Split split;

	private final InputContext inputContext;

	/**
	 * Instantiates a new input store object support.
	 *
	 * @param configuration the configuration
	 * @param basePath the base path
	 * @param codec the codec
	 */
	public InputStoreObjectSupport(Configuration configuration, Path basePath, CodecInfo codec) {
		this(configuration, basePath, codec, null);
	}

	/**
	 * Instantiates a new input store object support.
	 *
	 * @param configuration the configuration
	 * @param basePath the base path
	 * @param codec the codec
	 * @param split the input split
	 */
	public InputStoreObjectSupport(Configuration configuration, Path basePath, CodecInfo codec, Split split) {
		super(configuration, basePath, codec);
		this.split = split;
		if (split != null) {
			this.inputContext = new InputContext(split.getStart(), split.getEnd());
		} else {
			this.inputContext = new InputContext(0, Long.MAX_VALUE);
		}
	}

	/**
	 * Gets the input split.
	 *
	 * @return the input split
	 */
	public Split getSplit() {
		return split;
	}

	/**
	 * Gets the input context.
	 *
	 * @return the input context
	 */
	public InputContext getInputContext() {
		return inputContext;
	}

}
