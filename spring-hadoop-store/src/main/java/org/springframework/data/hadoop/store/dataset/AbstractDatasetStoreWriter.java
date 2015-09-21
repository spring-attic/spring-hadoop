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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Flushable;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.util.Assert;

/**
 * An abstract {@code DataStoreWriter} to be extended for providing Dataset writing
 * capabilities for specific use cases.
 *
 * @author Thomas Risberg
 * @author Janne Valkealahti
 *
 * @param <T> the type of entity to write
 * @param <R> the type of entity in writer
 *
 */
public abstract class AbstractDatasetStoreWriter<T, R> extends DatasetStoreObjectSupport implements DataStoreWriter<T> {

	private static final Log log = LogFactory.getLog(AbstractDatasetStoreWriter.class);

	private Class<T> entityClass;

	private DatasetRepositoryFactory datasetRepositoryFactory;

	private DatasetDefinition datasetDefinition;

	private DatasetWriter<R> writer;

	/** Sync lock for writer creation */
	private final Object lock = new Object();

	/**
	 * Instantiates a new {@code DataStoreWriter} for writing to a {@code org.kitesdk.data.Dataset}.
	 *
	 * @param entityClass the {@code Class} that the writer will write to the Dataset
	 * @param datasetRepositoryFactory the {@code DatasetRepositoryFactory} to be used for the writer
	 * @param datasetDefinition the {@code DatasetDefinition} to be used for the writer
	 */
	protected AbstractDatasetStoreWriter(Class<T> entityClass, DatasetRepositoryFactory datasetRepositoryFactory,
			DatasetDefinition datasetDefinition) {
		Assert.notNull(entityClass, "You must specify 'entityClass'");
		Assert.notNull(datasetRepositoryFactory, "You must provide a 'datasetRepositoryFactory'");
		Assert.notNull(datasetDefinition, "You must provide a 'datasetDefinition'");
		this.entityClass = entityClass;
		this.datasetRepositoryFactory = datasetRepositoryFactory;
		this.datasetDefinition = datasetDefinition;
	}

	@Override
	public void write(T entity) throws IOException {
		if (writer == null) {
			synchronized (lock) {
				if (writer == null) {
					writer = createWriter();
				}
			}
		}
		writer.write(convertEntity(entity));
		resetIdleTimeout();
	}

	@Override
	public void flush() throws IOException {
		if (log.isDebugEnabled()) {
			log.debug("Flushing writer " + writer);
		}
		if (writer != null && writer instanceof Flushable) {
			((Flushable) writer).flush();
		}
	}

	@Override
	public void close() throws IOException {
		if (log.isDebugEnabled()) {
			log.debug("Closing writer " + writer);
		}
		if (writer != null) {
			writer.close();
			writer = null;
		}
	}

	@Override
	protected void handleTimeout() {
		log.info("Timeout detected, closing writer");
		try {
			close();
		} catch (IOException e) {
		}
	}

	/**
	 * Convert entity to be written into a entity used
	 * by a writer.
	 *
	 * @param entity the entity
	 * @return the converted entity
	 */
	protected abstract R convertEntity(T entity);

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
	 * Creates a {@link DatasetWriter}.
	 *
	 * @return the dataset writer
	 */
	protected abstract DatasetWriter<R> createWriter();

}
