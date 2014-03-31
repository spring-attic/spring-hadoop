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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Formats;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.hadoop.store.StoreException;
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

	private DatasetDefinition defaultDatasetDefinition;

	/**
	 * The {@link DatasetRepositoryFactory} to use for this template.
	 * 
	 * @param datasetRepositoryFactory the DatasetRepositoryFactory to use
	 */
	public void setDatasetRepositoryFactory(DatasetRepositoryFactory datasetRepositoryFactory) {
		this.dsFactory = datasetRepositoryFactory;
	}

	/**
	 * The default {@link DatasetDefinition} to use for this template.
	 */
	public DatasetDefinition getDefaultDatasetDefinition() {
		return defaultDatasetDefinition;
	}

	/**
	 * The default {@link DatasetDefinition} to use for this template.
	 *
	 * @param defaultDatasetDefinition the DatasetDefinition to use
	 */
	public void setDefaultDatasetDefinition(DatasetDefinition defaultDatasetDefinition) {
		this.defaultDatasetDefinition = defaultDatasetDefinition;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(dsFactory, "The datasetRepositoryFactory property is required");
		if (defaultDatasetDefinition == null) {
			defaultDatasetDefinition = new DatasetDefinition(true);
		}
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
		if (Formats.PARQUET.getName().equals(defaultDatasetDefinition.getFormat().getName())) {
			return readGenericRecords(targetClass);
		} else {
			return readPojo(targetClass);
		}
	}

	private <T> Collection<T> readPojo(Class<T> targetClass) {
		DatasetReader<T> reader = getDataset(targetClass).newReader();
		List<T> results = new ArrayList<T>();
		try {
			reader.open();
			for (T r : reader) {
				results.add(r);
			}
		}
		finally {
			reader.close();
		}
		return results;
	}

	private <T> Collection<T> readGenericRecords(Class<T> targetClass) {
		Dataset<GenericRecord> dataset = getOrCreateDataset(targetClass, GenericRecord.class);
		DatasetReader<GenericRecord> reader = dataset.newReader();
		List<T> results = new ArrayList<T>();
		try {
			reader.open();
			for (GenericRecord r : reader) {
				T data = targetClass.newInstance();
				BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(data);
				for (Schema.Field f : r.getSchema().getFields()) {
					if (beanWrapper.isWritableProperty(f.name())) {
						beanWrapper.setPropertyValue(f.name(), r.get(f.name()));
					}
				}
				results.add(data);
			}
		} catch (InstantiationException e) {
			throw new StoreException("Unable to read records for class: " + targetClass.getName(), e);
		} catch (IllegalAccessException e) {
			throw new StoreException("Unable to read records for class: " + targetClass.getName(), e);
		} finally {
			reader.close();
		}
		return results;
	}

	@Override
	public <T> void write(Collection<T> records) {
		if (records == null || records.size() < 1) {
			return;
		}
		if (Formats.PARQUET.getName().equals(defaultDatasetDefinition.getFormat().getName())) {
			writeGenericRecords(records);
		} else {
			writePojo(records);
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

	private <T> void writePojo(Collection<T> records) {
		@SuppressWarnings("unchecked")
		Class<T> pojoClass = (Class<T>) records.iterator().next().getClass();
		Dataset<T> dataset = getOrCreateDataset(pojoClass, pojoClass);
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

	private <T> Dataset<T> getOrCreateDataset(Class<?> pojoClass, Class<T> recordClass) {
		String repoName = getDatasetName(pojoClass);
		DatasetDefinition datasetDefinition = getDefaultDatasetDefinition();
		Dataset<T> dataset;
		try {
			dataset = dsFactory.getDatasetRepository().load(repoName);
		}
		catch (DatasetNotFoundException ex) {
			Schema schema = datasetDefinition.getSchema(pojoClass);
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
				descriptor = new DatasetDescriptor.Builder()
						.schema(schema)
						.format(datasetDefinition.getFormat())
						.partitionStrategy(datasetDefinition.getPartitionStrategy())
						.build();
			}
			dataset = dsFactory.getDatasetRepository().create(repoName, descriptor);
		}
		return dataset;
	}

	private <T> void writeGenericRecords(Collection<T> records) {
		@SuppressWarnings("unchecked")
		Class<T> pojoClass = (Class<T>) records.iterator().next().getClass();
		Dataset<GenericRecord> dataset = getOrCreateDataset(pojoClass, GenericRecord.class);
		DatasetWriter<GenericRecord> writer = dataset.newWriter();
		try {
			writer.open();
			Schema schema = dataset.getDescriptor().getSchema();
			for (T record : records) {
				GenericRecordBuilder builder = new GenericRecordBuilder(schema);
				BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(record);
				for (Schema.Field f : schema.getFields()) {
					if (beanWrapper.isReadableProperty(f.name())) {
						Schema fieldSchema = f.schema();
						if (f.schema().getType().equals(Schema.Type.UNION)) {
							for (Schema s : f.schema().getTypes()) {
								if (!s.getName().equals("null")) {
									fieldSchema = s;
								}
							}
						}
						if (fieldSchema.getType().equals(Schema.Type.RECORD)) {
							throw new StoreException("Nested record currently not supported for field: " + f.name() +
							" of type: " + beanWrapper.getPropertyDescriptor(f.name()).getPropertyType().getName());
						} else {
							builder.set(f.name(), beanWrapper.getPropertyValue(f.name()));
						}
					}
				}
				try {
					writer.write(builder.build());
				} catch (ClassCastException cce) {
					throw new StoreException("Failed to write record with schema: " +
							schema, cce);
				}
			}
		} finally {
			writer.close();
		}

	}

	private <T> Dataset<T> getDataset(Class<T> clazz) {
		String repoName = getDatasetName(clazz);
		return dsFactory.getDatasetRepository().load(repoName);
	}

}
