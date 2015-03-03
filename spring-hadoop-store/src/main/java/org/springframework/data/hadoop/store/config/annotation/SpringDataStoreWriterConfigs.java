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
package org.springframework.data.hadoop.store.config.annotation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.partition.PartitionStrategy;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.RolloverStrategy;

/**
 * A holder object for all configured configs for Spring Hadoop.
 *
 * @author Janne Valkealahti
 *
 */
public class SpringDataStoreWriterConfigs {

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

	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public Path getBasePath() {
		return basePath;
	}

	public void setBasePath(Path basePath) {
		this.basePath = basePath;
	}

	public CodecInfo getCodec() {
		return codec;
	}

	public void setCodec(CodecInfo codec) {
		this.codec = codec;
	}

	public PartitionStrategy<?, ?> getPartitionStrategy() {
		return partitionStrategy;
	}

	public void setPartitionStrategy(PartitionStrategy<?, ?> partitionStrategy) {
		this.partitionStrategy = partitionStrategy;
	}

	public FileNamingStrategy getFileNamingStrategy() {
		return fileNamingStrategy;
	}

	public void setFileNamingStrategy(FileNamingStrategy fileNamingStrategy) {
		this.fileNamingStrategy = fileNamingStrategy;
	}

	public RolloverStrategy getRolloverStrategy() {
		return rolloverStrategy;
	}

	public void setRolloverStrategy(RolloverStrategy rolloverStrategy) {
		this.rolloverStrategy = rolloverStrategy;
	}

	public Boolean getOverwrite() {
		return overwrite;
	}

	public void setOverwrite(Boolean overwrite) {
		this.overwrite = overwrite;
	}

	public Boolean getAppendable() {
		return appendable;
	}

	public void setAppendable(Boolean appendable) {
		this.appendable = appendable;
	}

	public Long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(Long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public Long getCloseTimeout() {
		return closeTimeout;
	}

	public void setCloseTimeout(Long closeTimeout) {
		this.closeTimeout = closeTimeout;
	}

	public Integer getFileOpenAttempts() {
		return fileOpenAttempts;
	}

	public void setFileOpenAttempts(Integer fileOpenAttempts) {
		this.fileOpenAttempts = fileOpenAttempts;
	}

	public String getInWritingPrefix() {
		return inWritingPrefix;
	}

	public void setInWritingPrefix(String inWritingPrefix) {
		this.inWritingPrefix = inWritingPrefix;
	}

	public String getInWritingSuffix() {
		return inWritingSuffix;
	}

	public void setInWritingSuffix(String inWritingSuffix) {
		this.inWritingSuffix = inWritingSuffix;
	}

}
