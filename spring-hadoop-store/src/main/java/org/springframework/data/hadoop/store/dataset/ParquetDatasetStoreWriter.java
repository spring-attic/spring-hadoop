package org.springframework.data.hadoop.store.dataset;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Formats;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.data.hadoop.store.StoreException;
import org.springframework.util.Assert;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@code DataStoreWriter} for writing Datasets using the Parquet format.
 *
 * @author Thomas Risberg
 */
public class ParquetDatasetStoreWriter<T> extends AbstractDatasetStoreWriter<T> {

	private final static Log log = LogFactory.getLog(AvroPojoDatasetStoreWriter.class);

	protected volatile DatasetWriter<GenericRecord> writer;

	protected volatile Schema schema;

	private final Object monitor = new Object();

	/**
	 * Instantiates a new {@code DataStoreWriter} for writing Parquet records to a {@code org.kitesdk.data.Dataset}.
	 *
	 * @param entityClass the {@code Class} that the writer will write to the Dataset
	 * @param datasetRepositoryFactory the {@code DatasetRepositoryFactory} to be used for the writer
	 */
	public ParquetDatasetStoreWriter(Class<T> entityClass, DatasetRepositoryFactory datasetRepositoryFactory) {
		this(entityClass, datasetRepositoryFactory, new DatasetDefinition(entityClass, false, Formats.PARQUET.getName()));
	}

	/**
	 * Instantiates a new {@code DataStoreWriter} for writing Parquet records to a {@code org.kitesdk.data.Dataset}.
	 *
	 * @param entityClass the {@code Class} that the writer will write to the Dataset
	 * @param datasetRepositoryFactory the {@code DatasetRepositoryFactory} to be used for the writer
	 * @param datasetDefinition the {@code DatasetDefinition} to be used for the writer
	 */
	public ParquetDatasetStoreWriter(Class<T> entityClass, DatasetRepositoryFactory datasetRepositoryFactory, DatasetDefinition datasetDefinition) {
		super(entityClass, datasetRepositoryFactory, datasetDefinition);
	}

	@Override
	public void write(T entity) throws IOException {
		Assert.notNull(entity, "Entity to be written can't be 'null'.");
		if (!entity.getClass().equals(entityClass)) {
			throw new IllegalArgumentException("Entity to write is of class " + entity.getClass().getName() +
					". Expected " + entityClass.getName());
		}
		synchronized (monitor) {
			if (writer == null) {
				if (Formats.PARQUET.getName().equals(datasetDefinition.getFormat().getName())) {
					Dataset<GenericRecord> dataset =
							DatasetUtils.getOrCreateDataset(datasetRepositoryFactory, datasetDefinition, entityClass, GenericRecord.class);
					writer = dataset.newWriter();
					schema = dataset.getDescriptor().getSchema();
				} else {
					throw new StoreException("Invalid format " + datasetDefinition.getFormat() +
							" specified, you must use 'parquet' with " + this.getClass().getSimpleName() + ".");
				}
			}
		}
		GenericRecordBuilder builder = new GenericRecordBuilder(schema);
		BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(entity);
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
					if (fieldSchema.getType().equals(Schema.Type.BYTES)) {
						ByteBuffer buffer = null;
						Object value = beanWrapper.getPropertyValue(f.name());
						if (value == null || value instanceof byte[]) {
							if(value != null) {
								byte[] bytes = (byte[]) value;
								buffer = ByteBuffer.wrap(bytes);
							}
							builder.set(f.name(), buffer);
						} else {
							throw new StoreException("Don't know how to handle " + value.getClass() + " for " + fieldSchema);
						}
					} else {
						builder.set(f.name(), beanWrapper.getPropertyValue(f.name()));
				    }
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

	@Override
	public void flush() throws IOException {
		if (log.isDebugEnabled()) {
			log.debug("Flushing writer " + writer);
		}
		if (writer != null) {
			writer.flush();
		}
	}

	@Override
	public void close() throws IOException {
		if (log.isDebugEnabled()) {
			log.debug("Closing writer " + writer);
		}
		if (writer != null) {
			writer.close();
		}
	}
}
