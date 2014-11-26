/*
 * Copyright 2014 the original author or authors.
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.spi.filesystem.FileSystemProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a utility class in the store.dataset package. It provides static utility methods for working
 * with {@link org.kitesdk.data.Dataset}s, {@link org.kitesdk.data.DatasetReader}s and
 * {@link org.kitesdk.data.DatasetWriter}s
 *
 * @author Thomas Risberg
 * @author Janne Valkealahti
 * @since 2.0
 */
public abstract class DatasetUtils {

	private final static Log log = LogFactory.getLog(DatasetUtils.class);

	/**
	 * Gets the dataset name. This method simply delegates to
	 * {@link Class#getSimpleName()} and gets a lower case name.
	 *
	 * @param <T> the generic class type
	 * @param clazz the clazz
	 * @return the dataset name
	 */
	public static <T> String getDatasetName(Class<T> clazz) {
		return clazz.getSimpleName().toLowerCase();
	}

	/**
	 * Gets a {@link Dataset} using a {@link DatasetRepositoryFactory},
	 * {@link DatasetDefinition}, pojo class and a record class. {@link Dataset}
	 * is created if it doesn't exist.
	 *
	 * @param <T> the generic record class type
	 * @param dsFactory the dataset repository factory
	 * @param datasetDefinition the dataset definition
	 * @param pojoClass the pojo class
	 * @param recordClass the record class
	 * @return the dataset
	 */
	public static <T> Dataset<T> getOrCreateDataset(DatasetRepositoryFactory dsFactory, DatasetDefinition datasetDefinition,
													Class<?> pojoClass, Class<T> recordClass) {
		String repoName = getDatasetName(pojoClass);
		Dataset<T> dataset;
		try {
			dataset = dsFactory.getDatasetRepository().load(dsFactory.getNamespace(), repoName);
			log.debug("Found dataset for " + repoName);
		}
		catch (DatasetNotFoundException ex) {
			Schema schema = datasetDefinition.getSchema(pojoClass);
			log.debug("Creating dataset for " + repoName + " using schema " + schema);
			if (recordClass != null && recordClass.isAssignableFrom(GenericRecord.class)) {
				Schema genericSchema = Schema.createRecord(
						"Generic"+schema.getName(),
						"Generic representation of " + schema.getName(),
						schema.getNamespace(),
						false);
				List<Schema.Field> fields = new ArrayList<Schema.Field>();
				for (Schema.Field f : schema.getFields()) {
					fields.add(new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultValue()));
				}
				genericSchema.setFields(fields);
				schema = genericSchema;
			}
			DatasetDescriptor descriptor;
			if (datasetDefinition.getPartitionStrategy() == null) {
				descriptor = new DatasetDescriptor.Builder()
						.schema(schema)
						.format(datasetDefinition.getFormat())
						.compressionType(datasetDefinition.getCompressionType())
						.build();
			}
			else {
				if (log.isDebugEnabled()) {
					log.debug("Using partitioning: " + datasetDefinition.getPartitionStrategy());
				}
				DatasetDescriptor.Builder ddBuilder = new DatasetDescriptor.Builder()
						.schema(schema)
						.format(datasetDefinition.getFormat())
						.compressionType(datasetDefinition.getCompressionType())
						.partitionStrategy(datasetDefinition.getPartitionStrategy());
				if (datasetDefinition.getWriterCacheSize() != null) {
					ddBuilder = ddBuilder.property(FileSystemProperties.WRITER_CACHE_SIZE_PROP,
							Integer.toString(datasetDefinition.getWriterCacheSize()));
				}
				descriptor = ddBuilder.build();
				if (log.isDebugEnabled()) {
					log.debug("Using descriptor: " + descriptor);
				}
			}
			dataset = dsFactory.getDatasetRepository().create(dsFactory.getNamespace(), repoName, descriptor);
		}
		return dataset;
	}

	/**
	 * Gets the dataset using a {@link DatasetRepositoryFactory} and
	 * a pojo class. Passed class is a same used in {@link #getOrCreateDataset "pojoClass"}
	 *
	 * @param <T> the generic type
	 * @param dsFactory the ds factory
	 * @param pojoClass the pojo class
	 * @return the dataset
	 */
	public static <T> Dataset<T> getDataset(DatasetRepositoryFactory dsFactory, Class<T> pojoClass) {
		String repoName = getDatasetName(pojoClass);
		return dsFactory.getDatasetRepository().load(dsFactory.getNamespace(), repoName);
	}
}
