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
 * A {@code StaticBlockSplitter} is a {@link Splitter} handling
 * splits as relative of block sizes allowing to furthern split
 * blocks on a smaller chunks.
 * <p>
 * Having zero blocks means a split happens on a full block sizes,
 * furthermore using a splits sizes higher than zero simply means
 * how many times block is split. One simple means two splits for
 * a block, two means three splits for a block, etc.
 *
 * @author Janne Valkealahti
 *
 */
public class StaticBlockSplitter extends SlopBlockSplitter {

	private int splits;

	/**
	 * Instantiates a new static block splitter.
	 */
	public StaticBlockSplitter() {
		this(null, 0);
	}

	/**
	 * Instantiates a new static block splitter.
	 *
	 * @param configuration the configuration
	 */
	public StaticBlockSplitter(Configuration configuration) {
		this(configuration, 0);
	}

	/**
	 * Instantiates a new static block splitter.
	 *
	 * @param configuration the configuration
	 * @param splits the splits
	 */
	public StaticBlockSplitter(Configuration configuration, int splits) {
		super(configuration);
		this.splits = splits;
	}

	@Override
	protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
		if (splits > 0) {
			return blockSize / (splits + 1);
		} else {
			return blockSize;
		}
	}

	/**
	 * Sets the splits count.
	 *
	 * @param splits the new splits count
	 */
	public void setSplits(int splits) {
		this.splits = splits;
	}

}
