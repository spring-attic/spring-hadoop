/*
 * Copyright 2014-2015 the original author or authors.
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.PartitionDataStoreWriter;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.partition.PartitionStrategy;

/**
 * Implementation of a {@link PartitionDataStoreWriter} writing text data
 * using a partitioning.
 *
 * @author Janne Valkealahti
 * @author Rodrigo Meneses
 *
 * @param <K> the type of a partition key
 */
public class PartitionTextFileWriter<K> extends AbstractPartitionDataStoreWriter<String, K> {

	/**
	 * Instantiates a new text file partitioned writer.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 * @param partitionStrategy the partition strategy
	 */
	public PartitionTextFileWriter(Configuration configuration, Path basePath, CodecInfo codec,
			PartitionStrategy<String, K> partitionStrategy) {
		super(configuration, basePath, codec, partitionStrategy);
	}

	@Override
	protected DataStoreWriter<String> createWriter(Configuration configuration, final Path path, CodecInfo codec) {
		TextFileWriter writer = new TextFileWriter(getConfiguration(), path != null ? new Path(getBasePath(), path) : getBasePath(), codec) {
			@Override
			public synchronized void close() throws IOException {
				// catch close() and destroy from parent
				// this needs to happen before we pass
				// close() to writer
				destroyWriter(path);
				super.close();
				stop();
			}
		};
		if (getBeanFactory() != null) {
			writer.setBeanFactory(getBeanFactory());
		}
		writer.setPhase(getPhase());
		if (getTaskExecutor() != null) {
			writer.setTaskExecutor(getTaskExecutor());
		}
		if (getTaskScheduler() != null) {
			writer.setTaskScheduler(getTaskScheduler());
		}
		writer.setAutoStartup(isAutoStartup());
		if (getStoreEventPublisher() != null) {
			writer.setStoreEventPublisher(getStoreEventPublisher());
		}
		if (getFileNamingStrategyFactory() != null) {
			writer.setFileNamingStrategy(getFileNamingStrategyFactory().createInstance());
		}
		if (getRolloverStrategyFactory() != null) {
			writer.setRolloverStrategy(getRolloverStrategyFactory().createInstance());
		}
		writer.setIdleTimeout(getIdleTimeout());
		writer.setCloseTimeout(getCloseTimeout());
		writer.setFlushTimeout(getFlushTimeout());
		writer.setOverwrite(isOverwrite());
		writer.setAppendable(isAppendable());
		writer.setSyncable(isSyncable());
		writer.setInWritingPrefix(getInWritingPrefix());
		writer.setInWritingSuffix(getInWritingSuffix());
		writer.setMaxOpenAttempts(getMaxOpenAttempts());
		writer.afterPropertiesSet();
		writer.start();
		return writer;
	}

}
