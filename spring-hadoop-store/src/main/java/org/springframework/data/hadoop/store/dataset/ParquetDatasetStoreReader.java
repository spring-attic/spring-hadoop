/*
 * Copyright 2015 the original author or authors.
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
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

/**
 * A {@code DataStoreReader} for reading Datasets using the Parquet format.
 * 
 * @author Janne Valkealahti
 *
 * @param <T> the type of entity to read
 * 
 */
public class ParquetDatasetStoreReader<T> extends AbstractDatasetStoreReader<T, GenericRecord> {

	protected volatile Schema schema;
	
	/**
	 * Instantiates a new parquet dataset store reader.
	 *
	 * @param entityClass the entity class
	 * @param datasetRepositoryFactory the dataset repository factory
	 * @param datasetDefinition the dataset definition
	 */
	public ParquetDatasetStoreReader(Class<T> entityClass, DatasetRepositoryFactory datasetRepositoryFactory,
			DatasetDefinition datasetDefinition) {
		super(entityClass, datasetRepositoryFactory, datasetDefinition);
	}

	@Override
	protected DatasetReader<GenericRecord> createReader() {
		Dataset<GenericRecord> dataset = DatasetUtils.getOrCreateDataset(getDatasetRepositoryFactory(),
				getDatasetDefinition(), getEntityClass(), GenericRecord.class);
		schema = dataset.getDescriptor().getSchema();
		return dataset.newReader();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected T convertEntity(GenericRecord entity) {
		BeanWrapper beanWrapper = new BeanWrapperImpl(getEntityClass());
		for (Schema.Field f : schema.getFields()) {
			if (beanWrapper.isWritableProperty(f.name())) {
				beanWrapper.setPropertyValue(f.name(), entity.get(f.name()));
			}
		}
		return (T) beanWrapper.getWrappedInstance();
	}
	
}
