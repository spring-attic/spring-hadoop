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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Formats;
import org.kitesdk.data.RefinableView;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.hadoop.store.DataStoreWriter;
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

	private HashMap<String, DatasetDefinition> datasetDefinitions = new HashMap<String, DatasetDefinition>();

	public DatasetTemplate() {
	}

	public DatasetTemplate(DatasetRepositoryFactory dsFactory) {
		this.dsFactory = dsFactory;
	}

	public DatasetTemplate(DatasetRepositoryFactory dsFactory, DatasetDefinition defaultDatasetDefinition) {
		this.dsFactory = dsFactory;
		this.defaultDatasetDefinition = defaultDatasetDefinition;
	}

	/**
	 * The {@link DatasetRepositoryFactory} to use for this template.
	 *
	 * @param datasetRepositoryFactory the DatasetRepositoryFactory to use
	 */
	public void setDatasetRepositoryFactory(DatasetRepositoryFactory datasetRepositoryFactory) {
		this.dsFactory = datasetRepositoryFactory;
	}

	/**
	 * The default {@link DatasetDefinition} used for this template.
	 *
	 * @return the default dataset definition
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
		if (defaultDatasetDefinition.getTargetClass() != null) {
			datasetDefinitions.put(getDatasetName(defaultDatasetDefinition.getTargetClass()), defaultDatasetDefinition);
		}
	}

	/**
	 * The {@link DatasetDefinition}s used for this template.
	 *
	 * @return the dataset definitions
	 */
	public Collection<DatasetDefinition> getDatasetDefinitions() {
		return datasetDefinitions.values();
	}

	/**
	 * The list of {@link DatasetDefinition}s to use for this template.
	 *
	 * @param datasetDefinitions the DatasetDefinitions to use
	 */
	public void setDatasetDefinitions(Collection<DatasetDefinition> datasetDefinitions) {
		for (DatasetDefinition def : datasetDefinitions) {
			if (def.getTargetClass() != null) {
				this.datasetDefinitions.put(getDatasetName(def.getTargetClass()), def);
			} else {
				throw new StoreException("Target class is required for dataset definitions, invalid definition: " + def);
			}
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(dsFactory, "The datasetRepositoryFactory property is required");
		if (defaultDatasetDefinition == null) {
			defaultDatasetDefinition = new DatasetDefinition();
		}
	}

	@Override
	public <T> void read(Class<T> targetClass, RecordCallback<T> callback) {
		readWithCallback(targetClass, callback, null);
	}

	@Override
	public <T> Collection<T> read(Class<T> targetClass) {
		DatasetDescriptor descriptor = getDatasetDescriptor(targetClass);
		if (descriptor == null) {
			throw new StoreException("Unable to locate dataset for target class " + targetClass.getName());
		}
		if (Formats.PARQUET.equals(descriptor.getFormat())) {
			return readGenericRecords(targetClass, null);
		} else {
			return readPojo(targetClass, null);
		}
	}

	@Override
	public <T> void read(Class<T> targetClass, RecordCallback<T> callback, ViewCallback viewCallback) {
		readWithCallback(targetClass, callback, viewCallback);
	}

	@Override
	public <T> Collection<T> read(Class<T> targetClass, ViewCallback viewCallback) {
		DatasetDescriptor descriptor = getDatasetDescriptor(targetClass);
		if (descriptor == null) {
			throw new StoreException("Unable to locate dataset for target class " + targetClass.getName());
		}
		if (Formats.PARQUET.equals(descriptor.getFormat())) {
			return readGenericRecords(targetClass, viewCallback);
		} else {
			return readPojo(targetClass, viewCallback);
		}
	}

	private <T> void readWithCallback(Class<T> targetClass, RecordCallback<T> callback, ViewCallback viewCallback) {
		Dataset<T> dataset = DatasetUtils.getDataset(dsFactory, targetClass);
		if (dataset == null) {
			throw new StoreException("Unable to locate dataset for target class " + targetClass.getName());
		}
		DatasetReader<T> reader = null;
		if (viewCallback == null) {
			reader = dataset.newReader();
		} else {
			RefinableView<T> view = viewCallback.doInView(dataset, targetClass);
			if (view != null) {
				reader = view.newReader();
			}
		}
		if (reader != null) {
			try {
				for (T t : reader) {
					callback.doInRecord(t);
				}
			} finally {
				reader.close();
			}
		}
	}

	private <T> Collection<T> readPojo(Class<T> targetClass, ViewCallback viewCallback) {
		Dataset<T> dataset = DatasetUtils.getDataset(dsFactory, targetClass);
		if (dataset == null) {
			throw new StoreException("Unable to locate dataset for target class " + targetClass.getName());
		}
		DatasetReader<T> reader = null;
		if (viewCallback == null) {
			reader = dataset.newReader();
		} else {
			RefinableView<T> view = viewCallback.doInView(dataset, targetClass);
			if (view != null) {
				reader = view.newReader();
			}
		}
		List<T> results = new ArrayList<T>();
		if (reader != null) {
			try {
				for (T r : reader) {
					results.add(r);
				}
			}
			finally {
				reader.close();
			}
		}
		return results;
	}

	private <T> Collection<T> readGenericRecords(Class<T> targetClass, ViewCallback viewCallback) {
		Dataset<GenericRecord> dataset =
				DatasetUtils.getOrCreateDataset(dsFactory, getDatasetDefinitionToUseFor(targetClass), targetClass, GenericRecord.class);
		DatasetReader<GenericRecord> reader = null;
		if (viewCallback == null) {
			reader = dataset.newReader();
		} else {
			RefinableView<GenericRecord> view = viewCallback.doInView(dataset, GenericRecord.class);
			if (view != null) {
				reader = view.newReader();
			}
		}
		List<T> results = new ArrayList<T>();
		if (reader != null) {
			try {
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
		}
		return results;
	}

	@Override
	public <T> void write(Collection<T> records) {
		if (records == null || records.size() < 1) {
			return;
		}
		//TODO: add support for using Spring Data Commons MappingContext
		@SuppressWarnings("unchecked")
		Class<T> pojoClass = (Class<T>) records.iterator().next().getClass();
		DatasetDefinition datasetDefinition = getDatasetDefinitionToUseFor(pojoClass);
		DataStoreWriter<T> writer;
		if (Formats.PARQUET.getName().equals(datasetDefinition.getFormat().getName())) {
			writer = new ParquetDatasetStoreWriter<T>(pojoClass, dsFactory, datasetDefinition);
		} else {
			writer = new AvroPojoDatasetStoreWriter<T>(pojoClass, dsFactory, datasetDefinition);
		}
		try {
			for (T rec : records) {
				writer.write(rec);
			}
			writer.flush();
		} catch (IOException e) {
			throw new StoreException("Error writing " + pojoClass.getName(), e);
		} finally {
			try {
				writer.close();
			} catch (IOException ignore) {}
		}

	}

	@Override
	public void execute(DatasetRepositoryCallback callback) {
		callback.doInRepository(dsFactory.getDatasetRepository());
	}

	@Override
	public <T> DatasetDescriptor getDatasetDescriptor(Class<T> targetClass) {
		try {
			return DatasetUtils.getDataset(dsFactory, targetClass).getDescriptor();
		}
		catch (DatasetNotFoundException e) {
			return null;
		}
	}

	@Override
	public <T> String getDatasetName(Class<T> clazz) {
		return DatasetUtils.getDatasetName(clazz);
	}

	private DatasetDefinition getDatasetDefinitionToUseFor(Class<?> targetClass) {
		String datasetName = getDatasetName(targetClass);
		if (datasetDefinitions.containsKey(datasetName)) {
			return datasetDefinitions.get(datasetName);
		} else {
			return defaultDatasetDefinition;
		}
	}
}
