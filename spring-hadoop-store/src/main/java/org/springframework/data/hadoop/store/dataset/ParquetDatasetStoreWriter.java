/*
 * Copyright 2013-2015 the original author or authors.
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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Formats;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.data.hadoop.store.StoreException;
import org.springframework.util.Assert;

/**
 * A {@code DataStoreWriter} for writing Datasets using the Parquet format.
 * 
 * @author Thomas Risberg
 * @author Janne Valkealahti
 *
 * @param <T> the type of entity to write
 * 
 */
public class ParquetDatasetStoreWriter<T> extends AbstractDatasetStoreWriter<T, GenericRecord> {

	protected volatile Schema schema;

	/**
	 * Instantiates a new {@code DataStoreWriter} for writing Parquet records to a {@code org.kitesdk.data.Dataset}.
	 *
	 * @param entityClass the {@code Class} that the writer will write to the Dataset
	 * @param datasetRepositoryFactory the {@code DatasetRepositoryFactory} to be used for the writer
	 */
	public ParquetDatasetStoreWriter(Class<T> entityClass, DatasetRepositoryFactory datasetRepositoryFactory) {
		this(entityClass, datasetRepositoryFactory, new DatasetDefinition(entityClass, false, Formats.PARQUET.getName()));
	}

	/**
	 * Instantiates a new {@code DataStoreWriter} for writing Parquet records to a {@code org.kitesdk.data.Dataset}.
	 *
	 * @param entityClass the {@code Class} that the writer will write to the Dataset
	 * @param datasetRepositoryFactory the {@code DatasetRepositoryFactory} to be used for the writer
	 * @param datasetDefinition the {@code DatasetDefinition} to be used for the writer
	 */
	public ParquetDatasetStoreWriter(Class<T> entityClass, DatasetRepositoryFactory datasetRepositoryFactory, DatasetDefinition datasetDefinition) {
		super(entityClass, datasetRepositoryFactory, datasetDefinition);
	}

	@Override
	public void write(T entity) throws IOException {
		Assert.notNull(entity, "Entity to be written can't be 'null'.");
		if (!entity.getClass().equals(getEntityClass())) {
			throw new IllegalArgumentException("Entity to write is of class " + entity.getClass().getName() +
					". Expected " + getEntityClass().getName());
		}
		super.write(entity);
	}

	@Override
	protected DatasetWriter<GenericRecord> createWriter() {
		if (Formats.PARQUET.getName().equals(getDatasetDefinition().getFormat().getName())) {
			Dataset<GenericRecord> dataset =
					DatasetUtils.getOrCreateDataset(getDatasetRepositoryFactory(), getDatasetDefinition(), getEntityClass(), GenericRecord.class);
			schema = dataset.getDescriptor().getSchema();
			return dataset.newWriter();
		} else {
			throw new StoreException("Invalid format " + getDatasetDefinition().getFormat() +
					" specified, you must use 'parquet' with " + this.getClass().getSimpleName() + ".");
		}
	}
	
	@Override
	protected GenericRecord convertEntity(T entity) {
		if (entity instanceof GenericRecord) {
			return (GenericRecord) entity;
		}
		GenericRecordBuilder builder = new GenericRecordBuilder(schema);
		BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(entity);
		for (Schema.Field f : schema.getFields()) {
			if (beanWrapper.isReadableProperty(f.name())) {
				Schema fieldSchema = f.schema();
				if (f.schema().getType().equals(Schema.Type.UNION)) {
					for (Schema s : f.schema().getTypes()) {
						if (!s.getName().equals("null")) {
							fieldSchema = s;
						}
					}
				}
				if (fieldSchema.getType().equals(Schema.Type.RECORD)) {
					throw new StoreException("Nested record currently not supported for field: " + f.name() +
							" of type: " + beanWrapper.getPropertyDescriptor(f.name()).getPropertyType().getName());
				} else {
					if (fieldSchema.getType().equals(Schema.Type.BYTES)) {
						ByteBuffer buffer = null;
						Object value = beanWrapper.getPropertyValue(f.name());
						if (value == null || value instanceof byte[]) {
							if(value != null) {
								byte[] bytes = (byte[]) value;
								buffer = ByteBuffer.wrap(bytes);
							}
							builder.set(f.name(), buffer);
						} else {
							throw new StoreException("Don't know how to handle " + value.getClass() + " for " + fieldSchema);
						}
					} else {
						builder.set(f.name(), beanWrapper.getPropertyValue(f.name()));
				    }
				}
			}
		}
		try {
			return builder.build();			
		} catch (ClassCastException e) {
			throw new StoreException("Failed to write record with schema: " + schema, e);
		}
	}
	
}
