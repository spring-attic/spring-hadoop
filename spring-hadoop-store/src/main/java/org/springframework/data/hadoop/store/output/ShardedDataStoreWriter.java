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
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategyFactory;
import org.springframework.data.hadoop.store.strategy.rollover.RolloverStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.RolloverStrategyFactory;
import org.springframework.data.hadoop.store.support.OutputStoreObjectSupport;
import org.springframework.util.Assert;

/**
 * An implementation of {@link DataStoreWriter} which uses a collection of writers
 * to write data to different stores depending on the value of a supplied shardPath.
 *
 * @author Janne Valkealahti
 * @author Duncan McIntyre
 *
 * @param <T> the type of an entity to write
 */
public class ShardedDataStoreWriter<T> extends OutputStoreObjectSupport implements 
	DataStoreWriter<T> {

	private final static Log log = LogFactory.getLog(ShardedDataStoreWriter.class);

	/** Current shard writers identified by a path */
	private final Map<Path, DataStoreWriter<T>> writers = new ConcurrentHashMap<Path, DataStoreWriter<T>>();

	/** Writer for unknown shards */
	private DataStoreWriter<T> fallbackWriter;
	
	/** Factory for DataStoreWriters */
	private DataStoreWriterFactory<DataStoreWriter<T>> dataStoreWriterFactory;

	/** Reduced factory interface for naming strategy */
	private FileNamingStrategyFactory<FileNamingStrategy> fileNamingStrategyFactory;

	/** Reduced factory interface for rollover strategy */
	private RolloverStrategyFactory<RolloverStrategy> rolloverStrategyFactory;

	/** Max number of free file path open/find attempts guard against infinite loop */
	private int maxOpenAttempts = AbstractDataStreamWriter.DEFAULT_MAX_OPEN_ATTEMPTS;
	
	/**
	 * Instantiates a new sharded data store writer.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 * @param partitionStrategy the partition strategy
	 */
	public ShardedDataStoreWriter(Configuration configuration, Path basePath, CodecInfo codec,
			DataStoreWriterFactory<DataStoreWriter<T>> dataStoreWriterFactory) {
		
		super(configuration, basePath, codec);
		this.dataStoreWriterFactory = dataStoreWriterFactory;
		Assert.notNull(dataStoreWriterFactory, "Data store writer factory must be set");
	}

	@Override
	public void write(T entity) throws IOException {
		write(entity, null);
	}

	@Override
	public void flush() throws IOException {
		for (DataStoreWriter<?> writer : writers.values()) {
			try {
				writer.flush();
			} catch (Exception e) {
				log.warn("Writer caused exception in flush", e);
			}
		}
		if (fallbackWriter != null) {
			try {
				fallbackWriter.flush();
			} catch (Exception e) {
				log.warn("Writer caused exception in flush", e);
			}
		}
	}

	@Override
	public void close() throws IOException {
		for (DataStoreWriter<?> writer : writers.values()) {
			try {
				writer.close();
			} catch (Exception e) {
				log.warn("Writer caused exception in close", e);
			}
		}
		writers.clear();
		if (fallbackWriter != null) {
			try {
				fallbackWriter.close();
			} catch (Exception e) {
				log.warn("Writer caused exception in close", e);
			}
			fallbackWriter = null;
		}
	}

	protected synchronized void write(T entity, Path shardPath) throws IOException {
		
		DataStoreWriter<T> writer = null;
		
		Path path = null;
		
		if(shardPath == null) {
			path = getPath();
		} else {
			path = new Path(getPath(), shardPath);
		}

		// double sync for destroyWriter
		synchronized (writers) {
			
			if (path != null) {
				writer = writers.get(path);
			} else if (fallbackWriter == null) {
				fallbackWriter = writer = createWriter(getConfiguration(), null, getCodec());
			}
			
			if (writer == null) {
				writer = createWriter(getConfiguration(), path, getCodec());
				writers.put(path, writer);
			}
		}
		writer.write(entity);
	}

	@Override
	protected void onInit() throws Exception {
		super.onInit();
	}

	@Override
	protected void doStart() {
		super.doStart();
	}

	@Override
	protected void doStop() {
		try {
			flush();
			close();
		} catch (IOException e) {
		}
	}

	/**
	 * Sets the file naming strategy factory.
	 *
	 * @param fileNamingStrategyFactory the new file naming strategy factory
	 */
	public void setFileNamingStrategyFactory(FileNamingStrategyFactory<FileNamingStrategy> fileNamingStrategyFactory) {
		this.fileNamingStrategyFactory = fileNamingStrategyFactory;
	}

	/**
	 * Gets the file naming strategy factory.
	 *
	 * @return the file naming strategy factory
	 */
	public FileNamingStrategyFactory<FileNamingStrategy> getFileNamingStrategyFactory() {
		return fileNamingStrategyFactory;
	}

	/**
	 * Sets the rollover strategy factory.
	 *
	 * @param rolloverStrategyFactory the new rollover strategy factory
	 */
	public void setRolloverStrategyFactory(RolloverStrategyFactory<RolloverStrategy> rolloverStrategyFactory) {
		this.rolloverStrategyFactory = rolloverStrategyFactory;
	}

	/**
	 * Gets the rollover strategy factory.
	 *
	 * @return the rollover strategy factory
	 */
	public RolloverStrategyFactory<RolloverStrategy> getRolloverStrategyFactory() {
		return rolloverStrategyFactory;
	}
	
	/**
	 * Gets the data store writer factory
	 * 
	 * @return the data store writer factory
	 */
	public DataStoreWriterFactory<DataStoreWriter<T>> getDataStoreWriterFactory() {
		return dataStoreWriterFactory;
	}

	/**
	 * Sets the max open attempts.
	 *
	 * @param maxOpenAttempts the new max open attempts
	 */
	public void setMaxOpenAttempts(int maxOpenAttempts) {
		this.maxOpenAttempts = maxOpenAttempts;
	}

	/**
	 * Gets the max open attempts.
	 *
	 * @return the max open attempts
	 */
	public int getMaxOpenAttempts() {
		return maxOpenAttempts;
	}

	/**
	 * Destroys a writer with a given {@link Path} if exist.
	 * This method expects subclass to close and flush writer
	 * before call of this.
	 *
	 * @param path the path
	 */
	protected void destroyWriter(Path path) {
		log.info("Trying to destoy writer with path=[" + path + "]");
		if (path == null) {
			return;
		}
		// sync with writer create in write()
		synchronized (writers) {
			DataStoreWriter<T> writer = writers.remove(path);
			log.info("Removing writer=[" + writer + "]");
		}
	}
	
	protected DataStoreWriter<T> createWriter(Configuration configuration, final Path path, CodecInfo codec) {
		
		return dataStoreWriterFactory.createWriter(configuration, path, codec, this);

	}

}
