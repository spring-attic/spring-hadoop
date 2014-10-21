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

import java.util.ArrayList;
import java.util.List;

/**
 * This is a utility class in the store.dataset package. It provides static utility methods for working
 * with {@link org.kitesdk.data.Dataset}s, {@link org.kitesdk.data.DatasetReader}s and
 * {@link org.kitesdk.data.DatasetWriter}s
 *
 * @author Thomas Risberg
 * @since 2.0
 */
public abstract class DatasetUtils {

	private final static Log log = LogFactory.getLog(DatasetUtils.class);

	public static <T> String getDatasetName(Class<T> clazz) {
		return clazz.getSimpleName().toLowerCase();
	}

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
						.build();
			}
			else {
				if (log.isDebugEnabled()) {
					log.debug("Using partitioning: " + datasetDefinition.getPartitionStrategy());
				}
				descriptor = new DatasetDescriptor.Builder()
						.schema(schema)
						.format(datasetDefinition.getFormat())
						.partitionStrategy(datasetDefinition.getPartitionStrategy())
						.build();
			}
			dataset = dsFactory.getDatasetRepository().create(dsFactory.getNamespace(), repoName, descriptor);
		}
		return dataset;
	}

	public static <T> Dataset<T> getDataset(DatasetRepositoryFactory dsFactory, Class<T> clazz) {
		String repoName = getDatasetName(clazz);
		return dsFactory.getDatasetRepository().load(dsFactory.getNamespace(), repoName);
	}
}
