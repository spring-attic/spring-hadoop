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
package org.springframework.yarn.batch.partition;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.store.split.Split;
import org.springframework.data.hadoop.store.split.SplitLocation;
import org.springframework.util.StringUtils;
import org.springframework.yarn.batch.BatchSystemConstants;

/**
 * Base class providing share functionality for batch {@link Partitioner}.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractPartitioner implements Partitioner {

	private String keyPartition = BatchSystemConstants.KEY_PARTITION;

	private String keyFileName = BatchSystemConstants.KEY_FILENAME;

	private String keySplitStart = BatchSystemConstants.KEY_SPLITSTART;

	private String keySplitLength = BatchSystemConstants.KEY_SPLITLENGTH;

	private String keySplitLocations = BatchSystemConstants.KEY_SPLITLOCATIONS;

	private Configuration configuration;

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		return createPartitions();
	}

	/**
	 * The name of the base key for identifying partition's {@link ExecutionContext}.
	 * Defaults to "partition".
	 *
	 * @param keyPartition the value of the key
	 */
	public void setPartitionBaseIdentifier(String keyPartition) {
		this.keyPartition = keyPartition;
	}

	/**
	 * Gets the partition base identifier.
	 *
	 * @return the partition base identifier
	 */
	public String getPartitionBaseIdentifier() {
		return keyPartition;
	}

	/**
	 * The name of the key for the file name in each {@link ExecutionContext}.
	 * Defaults to "fileName".
	 *
	 * @param keyFileName the value of the key
	 */
	public void setKeyFileName(String keyFileName) {
		this.keyFileName = keyFileName;
	}

	/**
	 * Gets the key for a file name.
	 *
	 * @return the key for a file name
	 */
	public String getKeyFileName() {
		return keyFileName;
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
	 * Gets the key for split start.
	 *
	 * @return the key for split start
	 */
	public String getKeySplitStart() {
		return keySplitStart;
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
	 * Gets the key for split length.
	 *
	 * @return the key for split length
	 */
	public String getKeySplitLength() {
		return keySplitLength;
	}

	/**
	 * The name of the key for the split locations in each {@link ExecutionContext}.
	 * Defaults to "splitLocations".
	 *
	 * @param keySplitLocations the value of the key
	 */
	public void setKeySplitLocations(String keySplitLocations) {
		this.keySplitLocations = keySplitLocations;
	}

	/**
	 * Gets the key for split locations.
	 *
	 * @return the key for split locations
	 */
	public String getKeySplitLocations() {
		return keySplitLocations;
	}

	/**
	 * Sets the hadoop configuration.
	 *
	 * @param configuration the new hadoop configuration
	 */
	@Autowired(required=false)
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Gets the hadoop configuration.
	 *
	 * @return the hadoop configuration
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Creates the {@link ExecutionContext}s for partitions.
	 *
	 * @return the {@link ExecutionContext}s for partitions
	 */
	protected abstract Map<String, ExecutionContext> createPartitions();

	/**
	 * Creates the execution context.
	 *
	 * @param resource the resource
	 * @param split the split
	 * @return the execution context
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected ExecutionContext createExecutionContext(Resource resource, Split split) throws IOException {
		return createExecutionContext(new Path(resource.getURI()), split);
	}

	/**
	 * Creates the execution context.
	 *
	 * @param path the path
	 * @param split the split
	 * @return the execution context
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected ExecutionContext createExecutionContext(Path path, Split split) throws IOException {
		ExecutionContext context = new ExecutionContext();
		context.putString(getKeyFileName(), path.toUri().getPath());
		context.putLong(getKeySplitStart(), split.getStart());
		context.putLong(getKeySplitLength(), split.getLength());
		if (split instanceof SplitLocation) {
			context.putString(getKeySplitLocations(), StringUtils.collectionToCommaDelimitedString(Arrays
					.asList(((SplitLocation) split).getLocations())));
		}
		return context;
	}

}
