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
 * @author Janne Valkealahti
 *
 * @param <T> the type of an entity to write
 * @param <K> the type of a partition key
 */
public abstract class AbstractPartitionDataStoreWriter<T, K> extends LifecycleObjectSupport implements PartitionDataStoreWriter<T, K> {

	private final static Log log = LogFactory.getLog(AbstractPartitionDataStoreWriter.class);

	/** Hadoop configuration */
	private final Configuration configuration;

	/** Hdfs path into a store */
	private final Path basePath;

	/** Codec info for store */
	private final CodecInfo codec;

	/** Used partition strategy if any */
	private final PartitionStrategy<T, K> partitionStrategy;

	/** Current partition writers identified by a path */
	private final Map<Path, DataStoreWriter<T>> writers = new ConcurrentHashMap<Path, DataStoreWriter<T>>();

	/** Writer for unknown partitions */
	private DataStoreWriter<T> fallbackWriter;

	/** Reduced factory interface for naming strategy */
	private FileNamingStrategyFactory<FileNamingStrategy> fileNamingStrategyFactory;

	/** Reduced factory interface for rollover strategy */
	private RolloverStrategyFactory<RolloverStrategy> rolloverStrategyFactory;

	/** Idle timeout for writers */
	private long idleTimeout;

	/** Append flag for writers */
	private boolean append = false;

	/** Max number of free file path open/find attempts guard against infinite loop */
	private int maxOpenAttempts = AbstractDataStreamWriter.DEFAULT_MAX_OPEN_ATTEMPTS;

	/** Used in-writing suffix if any */
	private String suffix;

	/** Used in-writing prefix if any */
	private String prefix;

	/** Flag guarding if files can be overwritten */
	private boolean overwrite = false;

	/**
	 * Instantiates a new abstract data store partition writer.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 * @param partitionStrategy the partition strategy
	 */
	public AbstractPartitionDataStoreWriter(Configuration configuration, Path basePath, CodecInfo codec,
			PartitionStrategy<T, K> partitionStrategy) {
		super();
		this.configuration = configuration;
		this.basePath = basePath;
		this.codec = codec;
		this.partitionStrategy = partitionStrategy;
		Assert.notNull(partitionStrategy, "Partition strategy must be set");
	}

	@Override
	public void write(T entity) throws IOException {
		write(entity, partitionStrategy.getPartitionKeyResolver().resolvePartitionKey(entity));
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

	@Override
	public void write(T entity, K partitionKey) throws IOException {
		DataStoreWriter<T> writer = null;
		Path path = null;
		if (partitionKey != null) {
			path = partitionStrategy.getPartitionResolver().resolvePath(partitionKey);
			writer = writers.get(path);
		} else if (fallbackWriter == null){
			fallbackWriter = writer = createWriter(getConfiguration(), null, getCodec());
		}
		if (writer == null) {
			writer = createWriter(getConfiguration(), path, getCodec());
			writers.put(path, writer);
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
	 * Sets the idle timeout.
	 *
	 * @param idleTimeout the new idle timeout
	 */
	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

    /**
     * Sets the in writing suffix.
     *
     * @param suffix the new in writing suffix
     */
    public void setInWritingSuffix(String suffix) {
		this.suffix = suffix;
	}

    /**
     * Gets the in writing suffix.
     *
     * @return the in writing suffix
     */
    public String getInWritingSuffix() {
		return suffix;
	}

    /**
     * Sets the in writing prefix.
     *
     * @param prefix the new in writing prefix
     */
    public void setInWritingPrefix(String prefix) {
		this.prefix = prefix;
	}

    /**
     * Gets the in writing prefix.
     *
     * @return the in writing prefix
     */
    public String getInWritingPrefix() {
		return prefix;
	}

    /**
     * Sets the flag indicating if written files may be overwritten.
     * Default value is <code>FALSE</code> meaning {@code StoreException}
     * is thrown if file is about to get overwritten.
     *
     * @param overwrite the new overwrite
     */
    public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
		log.info("Setting overwrite to " + overwrite);
	}

	/**
	 * Checks if overwrite is enabled.
	 *
	 * @return true, if overwrite enabled
	 * @see #setOverwrite(boolean)
	 */
    public boolean isOverwrite() {
		return overwrite;
	}

	/**
	 * Checks if append is enabled.
	 *
	 * @return true, if append enabled
	 */
	public boolean isAppendable() {
		return append;
	}

	/**
	 * Set stream as append mode.
	 *
	 * @param append the append flag
	 */
	public void setAppendable(boolean append) {
		this.append = append;
	}

	/**
	 * Gets the idle timeout.
	 *
	 * @return the idle timeout
	 */
	public long getIdleTimeout() {
		return idleTimeout;
	}

	/**
	 * Gets the hadoop configuration.
	 *
	 * @return the configuration
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Gets the base path.
	 *
	 * @return the base path
	 */
	public Path getBasePath() {
		return basePath;
	}

	/**
	 * Gets the codec.
	 *
	 * @return the codec
	 */
	public CodecInfo getCodec() {
		return codec;
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
	 * Need to be implemented by a subclass for an actual writer.
	 *
	 * @param configuration the configuration
	 * @param basePath the base path
	 * @param codec the codec
	 * @return the data store writer
	 */
	protected abstract DataStoreWriter<T> createWriter(Configuration configuration, Path basePath, CodecInfo codec);

	/**
	 * Destroys a writer with a given {@link Path} if exist.
	 *
	 * @param path the path
	 */
	protected void destroyWriter(Path path) {
		if (path == null) {
			return;
		}
		DataStoreWriter<T> writer = writers.remove(path);
		if (writer != null) {
			try {
				writer.close();
			} catch (IOException e) {
			}
		}
	}

}
