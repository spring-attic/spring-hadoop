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

	private final Configuration configuration;

	private final Path basePath;

	private final CodecInfo codec;

	private PartitionStrategy<T, K> partitionStrategy;

	private final Map<Path, DataStoreWriter<T>> writers = new ConcurrentHashMap<Path, DataStoreWriter<T>>();

	private DataStoreWriter<T> fallbackWriter;

	private FileNamingStrategyFactory<FileNamingStrategy> fileNamingStrategyFactory;

	private RolloverStrategyFactory<RolloverStrategy> rolloverStrategyFactory;

	private long idleTimeout;

	private volatile int maxOpenAttempts = AbstractDataStreamWriter.DEFAULT_MAX_OPEN_ATTEMPTS;

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
			writer = createWriter(getConfiguration(), null, getCodec());
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

    public boolean isOverwrite() {
		return overwrite;
	}

	/**
	 * Gets the idle timeout.
	 *
	 * @return the idle timeout
	 */
	public long getIdleTimeout() {
		return idleTimeout;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public Path getBasePath() {
		return basePath;
	}

	public CodecInfo getCodec() {
		return codec;
	}

	public void setMaxOpenAttempts(int maxOpenAttempts) {
		this.maxOpenAttempts = maxOpenAttempts;
	}

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

}
