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

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Formats;
import org.springframework.data.hadoop.store.StoreException;
import org.springframework.util.Assert;

/**
 * A {@code DataStoreWriter} for writing Datasets using the Avro format.
 *
 * @author Thomas Risberg
 * @author Janne Valkealahti
 *
 */
public class AvroPojoDatasetStoreWriter<T> extends AbstractDatasetStoreWriter<T, T> {

	/**
	 * Instantiates a new {@code DataStoreWriter} for writing Avro records to a {@code org.kitesdk.data.Dataset}.
	 *
	 * @param entityClass the {@code Class} that the writer will write to the Dataset
	 * @param datasetRepositoryFactory the {@code DatasetRepositoryFactory} to be used for the writer
	 */
	public AvroPojoDatasetStoreWriter(Class<T> entityClass, DatasetRepositoryFactory datasetRepositoryFactory) {
		this(entityClass, datasetRepositoryFactory, new DatasetDefinition(entityClass, false, Formats.AVRO.getName()));
	}

	/**
	 * Instantiates a new {@code DataStoreWriter} for writing Avro records to a {@code org.kitesdk.data.Dataset}.
	 *
	 * @param entityClass the {@code Class} that the writer will write to the Dataset
	 * @param datasetRepositoryFactory the {@code DatasetRepositoryFactory} to be used for the writer
	 * @param datasetDefinition the {@code DatasetDefinition} to be used for the writer
	 */
	public AvroPojoDatasetStoreWriter(Class<T> entityClass, DatasetRepositoryFactory datasetRepositoryFactory, DatasetDefinition datasetDefinition) {
		super(entityClass, datasetRepositoryFactory, datasetDefinition);
	}

	@Override
	public void write(T entity) throws IOException {
		// pre-checks before write
		Assert.notNull(entity, "Entity to be written can't be 'null'.");
		if (!entity.getClass().equals(getEntityClass())) {
			throw new IllegalArgumentException("Entity to write is of class " + entity.getClass().getName()
					+ ". Expected " + getEntityClass().getName());
		}
		super.write(entity);
	}

	@Override
	protected DatasetWriter<T> createWriter() {
		if (Formats.AVRO.getName().equals(getDatasetDefinition().getFormat().getName())) {
			Dataset<T> dataset = DatasetUtils.getOrCreateDataset(getDatasetRepositoryFactory(), getDatasetDefinition(),
					getEntityClass(), getEntityClass());
			return dataset.newWriter();
		} else {
			throw new StoreException("Invalid format " + getDatasetDefinition().getFormat()
					+ " specified, you must use 'avro' with " + this.getClass().getSimpleName() + ".");
		}
	}
	
	@Override
	protected T convertEntity(T entity) {
		return entity;
	}

}
