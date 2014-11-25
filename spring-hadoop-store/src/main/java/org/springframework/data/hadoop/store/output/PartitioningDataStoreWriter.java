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
package org.springframework.data.hadoop.store.output;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.PartitionDataStoreWriter;
import org.springframework.data.hadoop.store.Serializer;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.partition.PartitionStrategy;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategyFactory;
import org.springframework.data.hadoop.store.strategy.rollover.RolloverStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.RolloverStrategyFactory;
import org.springframework.data.hadoop.store.support.LifecycleObjectSupport;
import org.springframework.util.Assert;

/**
 * Base implementation of {@link PartitionDataStoreWriter}.
 * 
 * Uses a {@link ShardedDataStoreWriter} to do the actual writing.
 *
 * @author Duncan McIntyre
 *
 * @param <T> the type of an entity to write
 * @param <K> the type of a partition key
 * @param <S> the type of the serialized entity
 */
public class PartitioningDataStoreWriter<T, K, S> implements PartitionDataStoreWriter<T, K, S> {

	private final static Log log = LogFactory.getLog(PartitioningDataStoreWriter.class);

	/** Used partition strategy if any */
	private final PartitionStrategy<T, K> partitionStrategy;

	/** The serializer to convert entities to the correct type for the data store writers */
	private Serializer<T,S> serializer;
	
	/** The actual writer to write the partitions as shards */
	private ShardedDataStoreWriter<S> shardWriter;

	/**
	 * Instantiates a new abstract data store partition writer.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 * @param partitionStrategy the partition strategy
	 */
	public PartitioningDataStoreWriter(ShardedDataStoreWriter<S> shardedWriter,
			PartitionStrategy<T, K> partitionStrategy, 
			Serializer<T,S> entitySerializer) {
		
		this.shardWriter = shardedWriter;
		Assert.notNull(shardedWriter, "Sharded writer must be set");
		this.partitionStrategy = partitionStrategy;
		Assert.notNull(partitionStrategy, "Partition strategy must be set");
		serializer = entitySerializer;
		Assert.notNull(serializer, "Serializer must be set");
	}
	
	public ShardedDataStoreWriter<S> getShardWriter() {
		return shardWriter;
	}

	@Override
	public void write(T entity) throws IOException {
		write(entity, partitionStrategy.getPartitionKeyResolver().resolvePartitionKey(entity));
	}

	@Override
	public void write(T entity, K partitionKey) throws IOException {
		
		Path path = null;
		
		if (partitionKey != null) {
			path = partitionStrategy.getPartitionResolver().resolvePath(partitionKey);
		}
		
		shardWriter.write(serializer.serialize(entity), path);
	}

	@Override
	public PartitionStrategy<T, K> getPartitionStrategy() {
		return partitionStrategy;
	}

	@Override
	public Serializer<T, S> getSerializer() {
		return serializer;
	}

	@Override
	public void close() throws IOException {
		shardWriter.close();
	}

	@Override
	public void flush() throws IOException {
		shardWriter.flush();
	}

}
