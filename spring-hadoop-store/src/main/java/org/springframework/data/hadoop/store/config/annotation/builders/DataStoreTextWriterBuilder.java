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
package org.springframework.data.hadoop.store.config.annotation.builders;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.data.hadoop.config.common.annotation.AbstractConfiguredAnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.AnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.ObjectPostProcessor;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.codec.Codecs;
import org.springframework.data.hadoop.store.config.annotation.SpringDataStoreWriterConfigs;
import org.springframework.data.hadoop.store.config.annotation.configurers.DefaultNamingStrategyConfigurer;
import org.springframework.data.hadoop.store.config.annotation.configurers.DefaultPartitionStrategyConfigurer;
import org.springframework.data.hadoop.store.config.annotation.configurers.DefaultRolloverStrategyConfigurer;
import org.springframework.data.hadoop.store.config.annotation.configurers.NamingStrategyConfigurer;
import org.springframework.data.hadoop.store.config.annotation.configurers.PartitionStrategyConfigurer;
import org.springframework.data.hadoop.store.config.annotation.configurers.RolloverStrategyConfigurer;
import org.springframework.data.hadoop.store.partition.PartitionStrategy;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.RolloverStrategy;

/**
 * {@link AnnotationBuilder} for {@link BeanDefinition} of a
 * {@link DataStoreWriter}.
 *
 * @author Janne Valkealahti
 *
 */
public final class DataStoreTextWriterBuilder
		extends
		AbstractConfiguredAnnotationBuilder<SpringDataStoreWriterConfigs, DataStoreTextWriterConfigurer, DataStoreTextWriterBuilder>
		implements DataStoreTextWriterConfigurer {

	private Configuration configuration;

	private Path basePath;

	private CodecInfo codec;

	private PartitionStrategy<?, ?> partitionStrategy;

	private FileNamingStrategy fileNamingStrategy;

	private RolloverStrategy rolloverStrategy;

	private Boolean overwrite;

	private Boolean appendable;

	private Long idleTimeout;

	private Long closeTimeout;

	private Integer fileOpenAttempts;

	private String inWritingPrefix;

	private String inWritingSuffix;

	/**
	 * Instantiates a new data store writer builder.
	 */
	public DataStoreTextWriterBuilder() {
	}

	/**
	 * Instantiates a new data store writer builder.
	 *
	 * @param objectPostProcessor the object post processor
	 */
	public DataStoreTextWriterBuilder(ObjectPostProcessor<Object> objectPostProcessor) {
		super(objectPostProcessor);
	}

	@Override
	protected SpringDataStoreWriterConfigs performBuild() throws Exception {
		SpringDataStoreWriterConfigs configs = new SpringDataStoreWriterConfigs();
		configs.setConfiguration(configuration);
		configs.setBasePath(basePath);
		configs.setCodec(codec);
		configs.setPartitionStrategy(partitionStrategy);
		configs.setFileNamingStrategy(fileNamingStrategy);
		configs.setRolloverStrategy(rolloverStrategy);
		configs.setOverwrite(overwrite);
		configs.setAppendable(appendable);
		configs.setIdleTimeout(idleTimeout);
		configs.setCloseTimeout(closeTimeout);
		configs.setFileOpenAttempts(fileOpenAttempts);
		configs.setInWritingPrefix(inWritingPrefix);
		configs.setInWritingSuffix(inWritingSuffix);
		return configs;
	}

	@Override
	public DataStoreTextWriterConfigurer configuration(Configuration configuration) {
		this.configuration = configuration;
		return this;
	}

	@Override
	public DataStoreTextWriterConfigurer basePath(Path basePath) {
		this.basePath = basePath;
		return this;
	}

	@Override
	public DataStoreTextWriterConfigurer basePath(String basePath) {
		return basePath(new Path(basePath));
	}

	@Override
	public DataStoreTextWriterConfigurer codec(CodecInfo codec) {
		this.codec = codec;
		return this;
	}

	@Override
	public DataStoreTextWriterConfigurer codec(String codec) {
		return codec(Codecs.getCodecInfo(codec));
	}

	@Override
	public DataStoreTextWriterConfigurer codec(Codecs codec) {
		return codec(codec.getCodecInfo());
	}

	@Override
	public DataStoreTextWriterConfigurer overwrite(boolean overwrite) {
		this.overwrite = overwrite;
		return this;
	}

	@Override
	public DataStoreTextWriterConfigurer append(boolean append) {
		this.appendable = append;
		return this;
	}

	@Override
	public DataStoreTextWriterConfigurer inWritingPrefix(String prefix) {
		this.inWritingPrefix = prefix;
		return this;
	}

	@Override
	public DataStoreTextWriterConfigurer inWritingSuffix(String suffix) {
		this.inWritingSuffix = suffix;
		return this;
	}

	@Override
	public DataStoreTextWriterConfigurer idleTimeout(long timeout) {
		this.idleTimeout = timeout;
		return this;
	}

	@Override
	public DataStoreTextWriterConfigurer closeTimeout(long timeout) {
		this.closeTimeout = timeout;
		return this;
	}

	@Override
	public DataStoreTextWriterConfigurer fileOpenAttempts(int attempts) {
		this.fileOpenAttempts = attempts;
		return this;
	}

	@Override
	public PartitionStrategyConfigurer withPartitionStrategy() throws Exception {
		return apply(new DefaultPartitionStrategyConfigurer());
	}

	@Override
	public NamingStrategyConfigurer withNamingStrategy() throws Exception {
		return apply(new DefaultNamingStrategyConfigurer());
	}

	@Override
	public RolloverStrategyConfigurer withRolloverStrategy() throws Exception {
		return apply(new DefaultRolloverStrategyConfigurer());
	}

	/**
	 * Sets the partition strategy for this builder.
	 *
	 * @param partitionStrategy the partition strategy
	 */
	public void setPartitionStrategy(PartitionStrategy<?, ?> partitionStrategy) {
		this.partitionStrategy = partitionStrategy;
	}

	/**
	 * Sets the file naming strategy for this builder.
	 *
	 * @param fileNamingStrategy the new file naming strategy
	 */
	public void setFileNamingStrategy(FileNamingStrategy fileNamingStrategy) {
		this.fileNamingStrategy = fileNamingStrategy;
	}

	/**
	 * Sets the rollover strategy for this builder.
	 *
	 * @param rolloverStrategy the new rollover strategy
	 */
	public void setRolloverStrategy(RolloverStrategy rolloverStrategy) {
		this.rolloverStrategy = rolloverStrategy;
	}

}
