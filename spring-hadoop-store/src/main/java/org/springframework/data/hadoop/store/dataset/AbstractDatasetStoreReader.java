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
package org.springframework.data.hadoop.store.dataset;

import java.io.IOException;

import org.kitesdk.data.DatasetReader;
import org.springframework.data.hadoop.store.DataStoreReader;
import org.springframework.data.hadoop.store.StoreException;
import org.springframework.util.Assert;

/**
 * An abstract {@code DataStoreReader} to be extended for providing Dataset reading
 * capabilities for specific use cases.
 * 
 * @author Janne Valkealahti
 *
 * @param <T> the type of entity to read
 * @param <R> the type of entity in reader
 * 
 */
public abstract class AbstractDatasetStoreReader<T, R> implements DataStoreReader<T> {

	private Class<T> entityClass;

	private DatasetRepositoryFactory datasetRepositoryFactory;

	private DatasetDefinition datasetDefinition;

	protected volatile DatasetReader<R> reader;

	private boolean closed = false;

	/**
	 * Instantiates a new abstract dataset store reader.
	 *
	 * @param entityClass the entity class
	 * @param datasetRepositoryFactory the dataset repository factory
	 * @param datasetDefinition the dataset definition
	 */
	protected AbstractDatasetStoreReader(Class<T> entityClass, DatasetRepositoryFactory datasetRepositoryFactory,
			DatasetDefinition datasetDefinition) {
		Assert.notNull(entityClass, "You must specify 'entityClass'");
		Assert.notNull(datasetRepositoryFactory, "You must provide a 'datasetRepositoryFactory'");
		Assert.notNull(datasetDefinition, "You must provide a 'datasetDefinition'");
		this.entityClass = entityClass;
		this.datasetRepositoryFactory = datasetRepositoryFactory;
		this.datasetDefinition = datasetDefinition;
	}

	@Override
	public T read() throws IOException {
		if (closed) {
			throw new StoreException("Reader is already closed");
		}
		if (reader == null) {
			reader = createReader();
		}
		if (reader.hasNext()) {
			return convertEntity(reader.next());
		} else {
			return null;
		}
	}

	@Override
	public void close() throws IOException {
		if (reader != null) {
			reader.close();
			reader = null;
			closed = true;
		}
	}

	/**
	 * Gets the entity class.
	 *
	 * @return the entity class
	 */
	protected Class<T> getEntityClass() {
		return entityClass;
	}

	/**
	 * Gets the dataset repository factory.
	 *
	 * @return the dataset repository factory
	 */
	protected DatasetRepositoryFactory getDatasetRepositoryFactory() {
		return datasetRepositoryFactory;
	}

	/**
	 * Gets the dataset definition.
	 *
	 * @return the dataset definition
	 */
	protected DatasetDefinition getDatasetDefinition() {
		return datasetDefinition;
	}
	
	/**
	 * Convert entity used by a reading into a entity
	 * returned.
	 * 
	 * @param entity the entity 
	 * @return the converted entity
	 */
	protected abstract T convertEntity(R entity);
	
	/**
	 * Creates a {@link DatasetReader}.
	 *
	 * @return the dataset reader
	 */
	protected abstract DatasetReader<R> createReader();

}
