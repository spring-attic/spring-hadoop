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

import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.util.Assert;

/**
 * An abstract {@code DataStoreWriter} to be extended for providing Dataset writing
 * capabilities for specific use cases.
 *
 * @author Thomas Risberg

 */
public abstract class AbstractDatasetStoreWriter<T> implements DataStoreWriter<T> {

	protected Class<T> entityClass;

	protected DatasetRepositoryFactory datasetRepositoryFactory;

	protected DatasetDefinition datasetDefinition;

	/**
	 * Instantiates a new {@code DataStoreWriter} for writing to a {@code org.kitesdk.data.Dataset}.
	 *
	 * @param entityClass the {@code Class} that the writer will write to the Dataset
	 * @param datasetRepositoryFactory the {@code DatasetRepositoryFactory} to be used for the writer
	 * @param datasetDefinition the {@code DatasetDefinition} to be used for the writer
	 */
	protected AbstractDatasetStoreWriter(Class<T> entityClass, DatasetRepositoryFactory datasetRepositoryFactory, DatasetDefinition datasetDefinition) {
		Assert.notNull(entityClass, "You must specify 'entityClass'");
		Assert.notNull(datasetRepositoryFactory, "You must provide a 'datasetRepositoryFactory'");
		Assert.notNull(datasetDefinition, "You must provide a 'datasetDefinition'");
		this.entityClass = entityClass;
		this.datasetRepositoryFactory = datasetRepositoryFactory;
		this.datasetDefinition = datasetDefinition;
	}

}
