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
package org.springframework.yarn.batch.partition;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

/**
 * Implementation of {@link Partitioner} that locates multiple resources and
 * associates their file names with execution context keys. Creates an
 * {@link ExecutionContext} per resource, and labels them as
 * <code>{partition0, partition1, ..., partitionN}</code>. The grid size is
 * ignored.
 *
 * @author Janne Valkealahti
 *
 */
public class MultiHdfsResourcePartitioner implements Partitioner {

	private static final String DEFAULT_KEY_FILENAME = "fileName";

	private static final String DEFAULT_KEY_SPLITSTART = "splitStart";

	private static final String DEFAULT_KEY_SPLITLENGTH = "splitLength";

	private static final String PARTITION_KEY = "partition";

	private String keyFileName = DEFAULT_KEY_FILENAME;

	private String keySplitStart = DEFAULT_KEY_SPLITSTART;

	private String keySplitLength = DEFAULT_KEY_SPLITLENGTH;

	private Resource[] resources = new Resource[0];

	/** Number of splits if file input is split */
	private int splitSize = 1;

	/** Flag indicating whether file input is partitioned */
	private boolean splitFile = true;

	/** Flag forcing split if input is less that block size */
	private boolean forceSplit = false;

	/**
	 * Flag for resource path format.
	 * @see #setUsePath(boolean)
	 */
	private boolean usePath = true;

	/** Yarn configuration */
	private Configuration configuration;

	/**
	 * The resources to assign to each partition. In Spring configuration you
	 * can use a pattern to select multiple resources.
	 * @param resources the resources to use
	 */
	public void setResources(Resource[] resources) {
		this.resources = resources;
	}

	/**
	 * The name of the key for the file name in each {@link ExecutionContext}.
	 * Defaults to "fileName".
	 * @param keyFileName the value of the key
	 */
	public void setKeyFileName(String keyFileName) {
		this.keyFileName = keyFileName;
	}

	/**
	 * The name of the key for the file split start in each {@link ExecutionContext}.
	 * Defaults to "splitStart".
	 *
	 * @param keySplitStart the value of the key
	 */
	public void setKeySplitStart(String keySplitStart) {
		this.keySplitStart = keySplitStart;
	}

	/**
	 * The name of the key for the file split length in each {@link ExecutionContext}.
	 * Defaults to "splitLength".
	 *
	 * @param keySplitLength the value of the key
	 */
	public void setKeySplitLength(String keySplitLength) {
		this.keySplitLength = keySplitLength;
	}

	/**
	 * Sets the input split size relative to
	 * block size of the HDFS file. Default value is
	 * 1 meaning if split is set to happen, every partition
	 * will handle exactly one block. Setting split size
	 * higher will allow better parallel handling
	 * of file input.
	 *
	 * @param splitSize the new split size
	 */
	public void setSplitSize(int splitSize) {
		this.splitSize = splitSize;
	}

	/**
	 * Sets the flat indicating if file input should be
	 * split. If split is set to happen, number of splits
	 * can be configured using {@link #setSplitSize(int)}.
	 * Default value is {@code TRUE}
	 *
	 * @param splitFile the new split file
	 */
	public void setSplitFile(boolean splitFile) {
		this.splitFile = splitFile;
	}

	/**
	 * Sets the force split. If set to {@code FALSE}
	 * input is forced to split if file size is
	 * below HDFS block size. Useful for testing and
	 * cases where processed data is very cpu intensive.
	 * Default value is {@code FALSE}
	 *
	 * @param forceSplit the new force split
	 */
	public void setForceSplit(boolean forceSplit) {
		this.forceSplit = forceSplit;
	}

	/**
	 * If set to true resource path is set using
	 * {@link java.net.URL#getPath()}, otherwise
	 * {@link java.net.URL#toExternalForm()} is used.
	 * Default value is {@code TRUE}.
	 *
	 * @param usePath whether path part is used
	 */
	public void setUsePath(boolean usePath) {
		this.usePath = usePath;
	}

	/**
	 * Sets the Yarn configuration.
	 *
	 * @param configuration the new Yarn configuration
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Assign the filename of each of the injected resources to an
	 * {@link ExecutionContext}.
	 *
	 * @see Partitioner#partition(int)
	 */
	public Map<String, ExecutionContext> partition(int gridSize) {
		Map<String, ExecutionContext> contexts = new HashMap<String, ExecutionContext>();

		try {
			FileSystem fs = FileSystem.get(configuration);
			int i = 0;
			for (Resource resource : resources) {
				Assert.state(resource.exists(), "Resource does not exist: " + resource);
				Path path = new Path(resource.getURL().getPath());

				FileStatus[] fileStatuses = fs.globStatus(path);
				if (fileStatuses == null || fileStatuses.length != 1) {
					throw new IllegalArgumentException("Error getting file status for resource=" + resource);
				}

				if (splitFile) {
					// get split size and estimate split blocks
					long blockSize = fileStatuses[0].getBlockSize();
					long[] positions = getSplitPositions(blockSize, fileStatuses[0].getLen(), splitSize, forceSplit);
					long position = 0;
					for (int j = 0; j < positions.length; j++) {
						contexts.put(PARTITION_KEY + i++, createExecutionContext(resource, position, positions[j]-position-1));
						position = positions[j];
					}
					contexts.put(PARTITION_KEY + i++, createExecutionContext(resource, position, fileStatuses[0].getLen()-position));
				} else {
					// just add file
					contexts.put(PARTITION_KEY + i++, createExecutionContext(resource, 0, fileStatuses[0].getLen()));
				}
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("Error partitioning splits", e);
		}

		return contexts;
	}

	/**
	 * Gets the split positions.
	 *
	 * @param blockSize the block size
	 * @param total the total
	 * @param splitSize the split size
	 * @param forceSplit the force split
	 * @return the split positions
	 */
	private static long[] getSplitPositions(long blockSize, long total, int splitSize, boolean forceSplit) {
		int blocks;
		long blockSplitSize;

		if (forceSplit && blockSize > total) {
			blocks = splitSize;
			blockSplitSize = total / (splitSize+1);
		} else {
			blocks = (int) (total/blockSize) * splitSize;
			blockSplitSize = blockSize / splitSize;
		}

		long[] positions = new long[blocks];
		long position = blockSplitSize;
		for (int i = 0; i<blocks; i++) {
			positions[i] = position;
			position += blockSplitSize;
		}
		return positions;
	}

	/**
	 * Creates the execution context.
	 *
	 * @param resource the resource
	 * @param fileName the file name
	 * @param splitStart the split start
	 * @param splitLength the split length
	 * @return the execution context
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private ExecutionContext createExecutionContext(Resource resource, long splitStart, long splitLength)
			throws IOException {
		ExecutionContext context = new ExecutionContext();
		if (usePath) {
			context.putString(keyFileName, resource.getURL().getPath());
		} else {
			context.putString(keyFileName, resource.getURL().toExternalForm());
		}
		context.putLong(keySplitStart, splitStart);
		context.putLong(keySplitLength, splitLength);
		return context;
	}

}
