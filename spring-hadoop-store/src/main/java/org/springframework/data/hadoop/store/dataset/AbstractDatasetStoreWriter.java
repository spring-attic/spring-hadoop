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
