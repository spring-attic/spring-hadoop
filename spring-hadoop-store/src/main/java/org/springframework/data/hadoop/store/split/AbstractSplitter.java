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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * A base class for {@code Splitter} implementations.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractSplitter implements Splitter {

	private Configuration configuration;

	/**
	 * Instantiates a new abstract splitter.
	 */
	public AbstractSplitter() {

	}

	/**
	 * Instantiates a new abstract splitter.
	 *
	 * @param configuration the configuration
	 */
	public AbstractSplitter(Configuration configuration) {
		this.configuration = configuration;
	}

	@Override
	public abstract List<Split> getSplits(Path path) throws IOException;

	/**
	 * Gets the hadoop configuration.
	 *
	 * @return the hadoop configuration
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Sets the configuration.
	 *
	 * @param configuration the new configuration
	 */
	@Autowired(required=false)
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}


	/**
	 * Compute split size. Default implementation takes minSize if it is bigger
	 * than minimum from a maxSize or blockSize.
	 *
	 * @param blockSize the block size
	 * @param minSize the min size
	 * @param maxSize the max size
	 * @return the long
	 */
	protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
		return Math.max(minSize, Math.min(maxSize, blockSize));
	}

	/**
	 * Gets the block index.
	 *
	 * @param blocks the blk locations
	 * @param offset the offset
	 * @return the block index
	 * @throws IllegalArgumentException if offset is outside of blocks
	 */
	protected int getBlockIndex(BlockLocation[] blocks, long offset) {
		for (int i = 0; i < blocks.length; i++) {
			if ((blocks[i].getOffset() <= offset) && (offset < blocks[i].getOffset() + blocks[i].getLength())) {
				return i;
			}
		}
		BlockLocation block = blocks[blocks.length - 1];
		long length = block.getOffset() + block.getLength() - 1;
		throw new IllegalArgumentException("Offset " + offset + " is outside of file with length=" + length);
	}

	/**
	 * Builds the split.
	 *
	 * @param start the start
	 * @param length the length
	 * @param hosts the hosts
	 * @return the split
	 */
	protected Split buildSplit(long start, long length, String[] hosts) {
		return new GenericSplit(start, length, hosts);
	}

}