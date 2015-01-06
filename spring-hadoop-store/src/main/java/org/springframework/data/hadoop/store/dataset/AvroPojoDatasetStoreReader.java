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

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;

/**
 * A {@code DataStoreReader} for reading Datasets using the Avro format.
 * 
 * @author Janne Valkealahti
 *
 * @param <T> the type of entity to write
 * 
 */
public class AvroPojoDatasetStoreReader<T> extends AbstractDatasetStoreReader<T, T> {

	public AvroPojoDatasetStoreReader(Class<T> entityClass, DatasetRepositoryFactory datasetRepositoryFactory,
			DatasetDefinition datasetDefinition) {
		super(entityClass, datasetRepositoryFactory, datasetDefinition);
	}

	@Override
	protected T convertEntity(T entity) {
		return entity;
	}

	@Override
	protected DatasetReader<T> createReader() {
		Dataset<T> dataset = DatasetUtils.getOrCreateDataset(getDatasetRepositoryFactory(), getDatasetDefinition(),
		getEntityClass(), getEntityClass());
		return dataset.newReader();
	}
		

}
