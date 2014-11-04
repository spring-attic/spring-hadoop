/*
 * Copyright 2013-2014 the original author or authors.
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

package org.springframework.data.hadoop.store.dataset;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;
import org.springframework.data.hadoop.store.StoreException;
import org.springframework.util.Assert;

/**
 * Class to define the options for a {@link org.kitesdk.data.Dataset}
 *
 * @author Thomas Risberg
 * @since 2.0
 */
public class DatasetDefinition{

	private Class<?> targetClass;

	private boolean allowNullValues;

	private Format format;

	private PartitionStrategy partitionStrategy;

	private Integer writerCacheSize;

	public DatasetDefinition() {
		this(null, true, Formats.AVRO.getName());
	}

	public DatasetDefinition(boolean allowNullValues) {
		this(null, allowNullValues, Formats.AVRO.getName());
	}

	public DatasetDefinition(boolean allowNullValues, String format) {
		this(null, allowNullValues, format);
	}

	public DatasetDefinition(Class<?> targetClass, boolean allowNullValues, String format) {
		setTargetClass(targetClass);
		setAllowNullValues(allowNullValues);
		setFormat(format);
	}

	public DatasetDefinition(Class<?> targetClass, PartitionStrategy partitionStrategy) {
		this(targetClass, Formats.AVRO.getName(), partitionStrategy);
	}

	public DatasetDefinition(Class<?> targetClass, String format, PartitionStrategy partitionStrategy) {
		setTargetClass(targetClass);
		setAllowNullValues(false);
		setFormat(format);
		setPartitionStrategy(partitionStrategy);
	}

	public void setTargetClass(Class<?> targetClass) {
		this.targetClass = targetClass;
	}

	public void setAllowNullValues(boolean allowNullValues) {
		this.allowNullValues = allowNullValues;
	}

	public void setFormat(String format) {
		Assert.notNull(format, "The format can't be null");
		try {
			this.format = Formats.fromString(format);
		} catch (IllegalArgumentException e) {
			throw new StoreException("Invalid format '" + format + "' specified", e);
		}
	}

	public void setPartitionStrategy(PartitionStrategy partitionStrategy) {
		this.partitionStrategy = partitionStrategy;
	}

	public void setWriterCacheSize(Integer writerCacheSize) {
		this.writerCacheSize = writerCacheSize;
	}

	public Class<?> getTargetClass() {
		return targetClass;
	}

	public boolean isAllowNullValues() {
		return allowNullValues;
	}

	public Format getFormat() {
		return format;
	}

	public PartitionStrategy getPartitionStrategy() {
		return partitionStrategy;
	}

	public Integer getWriterCacheSize() {
		return writerCacheSize;
	}

	public Schema getSchema(Class<?> datasetClass) {
		Schema schema;
		if (allowNullValues) {
			schema = ReflectData.AllowNull.get().getSchema(datasetClass);
		} else {
			schema = ReflectData.get().getSchema(datasetClass);
		}
		return schema;
	}
}
