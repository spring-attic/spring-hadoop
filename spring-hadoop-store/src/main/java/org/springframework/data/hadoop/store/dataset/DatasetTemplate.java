/*
 * Copyright 2013 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.PartitionStrategy;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * This is the central class in the store.dataset package. It simplifies the use of {@link Dataset}s,
 * {@link DatasetReader}s and {@link DatasetWriter}s
 *
 * @author Thomas Risberg
 * @since 2.0
 */
public class DatasetTemplate implements InitializingBean, DatasetOperations {

	private DatasetRepositoryFactory dsFactory;

	/**
	 * The {@link DatasetRepositoryFactory} to use for this template.
	 * 
	 * @param datasetRepositoryFactory the DatasetRepositoryFactory to use
	 */
	public void setDatasetRepositoryFactory(DatasetRepositoryFactory datasetRepositoryFactory) {
		this.dsFactory = datasetRepositoryFactory;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(dsFactory, "The datasetRepositoryFactory property is required");
	}

	@Override
	public <T> void read(Class<T> targetClass, RecordCallback<T> callback) {
		DatasetReader<T> reader = getDataset(targetClass).newReader();
		try {
			reader.open();
			for (T t : reader) {
				callback.doInRecord(t);
			}
		}
		finally {
			reader.close();
		}
	}

	@Override
	public <T> Collection<T> read(Class<T> targetClass) {
		DatasetReader<T> reader = getDataset(targetClass).newReader();
		List<T> results = new ArrayList<T>();
		try {
			reader.open();
			for (T t : reader) {
				results.add(t);
			}
		}
		finally {
			reader.close();
		}
		return results;
	}

	@Override
	public <T> void write(Collection<T> records) {
		write(records, null);
	}

	@Override
	public <T> void write(Collection<T> records, PartitionStrategy partitionStrategy) {
		if (records == null || records.size() < 1) {
			return;
		}
		@SuppressWarnings("unchecked")
		Class<T> recordClass = (Class<T>) records.iterator().next().getClass();
		Dataset<T> dataset = getOrCreateDataset(recordClass, partitionStrategy);
		DatasetWriter<T> writer = dataset.newWriter();
		try {
			writer.open();
			for (T record : records) {
				writer.write(record);
			}
		}
		finally {
			writer.close();
		}
	}

	@Override
	public void execute(DatasetRepositoryCallback callback) {
		callback.doInRepository(dsFactory.getDatasetRepository());
	}

	@Override
	public <T> String getDatasetName(Class<T> clazz) {
		return clazz.getSimpleName().toLowerCase();
	}

	private <T> Dataset<T> getOrCreateDataset(Class<T> clazz, PartitionStrategy partitionStrategy) {
		String repoName = getDatasetName(clazz);
		Dataset<T> dataset;
		try {
			dataset = dsFactory.getDatasetRepository().load(repoName);
		}
		catch (DatasetNotFoundException ex) {
			Schema schema = ReflectData.AllowNull.get().getSchema(clazz);
			DatasetDescriptor descriptor;
			if (partitionStrategy == null) {
				descriptor = new DatasetDescriptor.Builder().schema(schema).build();
			}
			else {
				descriptor =
						new DatasetDescriptor.Builder().schema(schema).partitionStrategy(partitionStrategy).build();
			}
			dataset = dsFactory.getDatasetRepository().create(repoName, descriptor);
		}
		return dataset;
	}

	private <T> Dataset<T> getDataset(Class<T> clazz) {
		String repoName = getDatasetName(clazz);
		return dsFactory.getDatasetRepository().load(repoName);
	}

}
