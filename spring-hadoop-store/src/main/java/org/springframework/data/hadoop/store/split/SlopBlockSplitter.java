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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A {@code SlopBlockSplitter} is a {@link Splitter} which
 * roughly splitting at least on block boundaries allowing
 * last block to be combined with previous if its size is
 * too small. Behaviour of how big this last block overflow
 * can be is controlled by a slop factor.
 * <p>
 * Default slop factor is 1.1 which allows last block to
 * overflow by 10%.
 *
 * @author Janne Valkealahti
 *
 */
public class SlopBlockSplitter extends AbstractSplitter {

	protected static final double DEFAULT_SPLIT_SLOP = 1.1;

	/** The min split size. */
	private long minSplitSize = 1l;

	/** The max split size. */
	private long maxSplitSize = Long.MAX_VALUE;

	private double slop = DEFAULT_SPLIT_SLOP;

	/**
	 * Instantiates a new slop block splitter.
	 */
	public SlopBlockSplitter() {
		super();
	}

	/**
	 * Instantiates a new slop block splitter.
	 *
	 * @param configuration the configuration
	 */
	public SlopBlockSplitter(Configuration configuration) {
		super(configuration);
	}

	/**
	 * Instantiates a new slop block splitter.
	 *
	 * @param configuration the configuration
	 * @param minSplitSize the min split size
	 * @param maxSplitSize the max split size
	 */
	public SlopBlockSplitter(Configuration configuration, long minSplitSize, long maxSplitSize) {
		super(configuration);
		setMinSplitSize(minSplitSize);
		setMaxSplitSize(maxSplitSize);
	}

	/**
	 * Instantiates a new slop block splitter.
	 *
	 * @param configuration the configuration
	 * @param minSplitSize the min split size
	 * @param maxSplitSize the max split size
	 * @param slop the slop factor
	 */
	public SlopBlockSplitter(Configuration configuration, long minSplitSize, long maxSplitSize, double slop) {
		super(configuration);
		setMinSplitSize(minSplitSize);
		setMaxSplitSize(maxSplitSize);
		setSlop(slop);
	}

	@Override
	public List<Split> getSplits(Path path) throws IOException {
		List<Split> splits = new ArrayList<Split>();

		FileSystem fs = path.getFileSystem(getConfiguration());
		FileStatus status = fs.getFileStatus(path);

		long length = status.getLen();
		BlockLocation[] blocks = fs.getFileBlockLocations(status, 0, length);

		long blockSize = status.getBlockSize();
		long splitSize = computeSplitSize(blockSize, getMinSplitSize(), getMaxSplitSize());

		long remaining = length;
		while (((double) remaining) / splitSize > slop) {
			int i = getBlockIndex(blocks, length - remaining);
			splits.add(buildSplit(length - remaining, splitSize, blocks[i].getHosts()));
			remaining -= splitSize;
		}

		if (remaining != 0) {
			int blkIndex = getBlockIndex(blocks, length - remaining);
			splits.add(buildSplit(length - remaining, remaining,
					blocks[blkIndex].getHosts()));
		}

		return splits;
	}

	/**
	 * Gets the minimum split size.
	 *
	 * @return the minimum split size
	 */
	public long getMinSplitSize() {
		return minSplitSize;
	}

	/**
	 * Sets the minimum split size.
	 *
	 * @param minSplitSize the new minimum split size
	 */
	public void setMinSplitSize(long minSplitSize) {
		this.minSplitSize = minSplitSize;
	}

	/**
	 * Gets the maximum split size.
	 *
	 * @return the maximum split size
	 */
	public long getMaxSplitSize() {
		return maxSplitSize;
	}

	/**
	 * Sets the maximum split size.
	 *
	 * @param maxSplitSize the new maximum split size
	 */
	public void setMaxSplitSize(long maxSplitSize) {
		this.maxSplitSize = maxSplitSize;
	}

	/**
	 * Sets the slop factor.
	 *
	 * @param slop the new slop factor
	 */
	public void setSlop(double slop) {
		this.slop = slop;
	}

}