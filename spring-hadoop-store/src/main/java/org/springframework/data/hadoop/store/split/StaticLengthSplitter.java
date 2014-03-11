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

import org.apache.hadoop.conf.Configuration;

/**
 * A {@code StaticLengthSplitter} is a {@link Splitter} using
 * a static length to split a resource into an equal sized. Last
 * remaining block split is returned as it is.
 *
 * @author Janne Valkealahti
 *
 */
public class StaticLengthSplitter extends SlopBlockSplitter {

	/**
	 * Instantiates a new static length splitter.
	 *
	 * @param length the length
	 */
	public StaticLengthSplitter(long length) {
		this(null, length);
	}

	/**
	 * Instantiates a new static length splitter.
	 *
	 * @param configuration the configuration
	 * @param length the length
	 */
	public StaticLengthSplitter(Configuration configuration, long length) {
		super(configuration, length, length, 1);
	}

	/**
	 * Sets the split length.
	 *
	 * @param length the new length length
	 */
	public void setLength(long length) {
		setMinSplitSize(length);
		setMaxSplitSize(length);
	}

}
