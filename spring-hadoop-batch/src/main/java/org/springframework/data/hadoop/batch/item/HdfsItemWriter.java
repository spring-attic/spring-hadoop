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
package org.springframework.data.hadoop.batch.item;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.serializer.Serializer;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * An {@link ItemWriter} implementation used to write the incoming items to
 * HDFS.  Due to the HDFS limitation that files cannot be deleted or modified, the
 * ability to roll back a file to a previously known state is not possible.  This
 * prevents the ability to restart using this {@link ItemWriter}.
 * <br>
 * This {@link ItemWriter} is <em>not</em> thread-safe.
 *
 * @author Michael Minella
 */
public class HdfsItemWriter<T> implements ItemStreamWriter<T> {

	private static final String BUFFER_KEY_PREFIX = HdfsItemWriter.class.getName() + ".BUFFER_KEY";
	private final String bufferKey;
	private String fileName;
	private FileSystem fileSystem;
	private FSDataOutputStream fsDataOutputStream;
	private Serializer<T> itemSerializer;

	/**
	 * Constructor
	 *
	 * @param fileSystem - HDFS {@link FileSystem} reference
	 * @param itemSerializer - Strategy for serializing items
	 * @param fileName - Name of the file to be written to
	 */
	public HdfsItemWriter(FileSystem fileSystem, Serializer<T> itemSerializer, String fileName) {
		Assert.notNull(fileSystem, "Hadoop FileSystem is required.");
		Assert.notNull(itemSerializer, "A Serializer implementation is required");
		Assert.isTrue(StringUtils.hasText(fileName), "A non-empty fileName is required.");
		this.fileSystem = fileSystem;
		this.bufferKey = BUFFER_KEY_PREFIX + "." + hashCode();
		this.itemSerializer = itemSerializer;
		this.fileName = fileName;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private List<? extends T> getCurrentBuffer() {
		if(!TransactionSynchronizationManager.hasResource(bufferKey)) {
			TransactionSynchronizationManager.bindResource(bufferKey, new ArrayList());

			TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
				@Override
				public void beforeCommit(boolean readOnly) {
					List items = (List) TransactionSynchronizationManager.getResource(bufferKey);

					if(!CollectionUtils.isEmpty(items)) {
						if(!readOnly) {
							doWrite(items);
						}
					}
				}

				@Override
				public void afterCompletion(int status) {
					if(TransactionSynchronizationManager.hasResource(bufferKey)) {
						TransactionSynchronizationManager.unbindResource(bufferKey);
					}
				}
			});
		}

		return (List) TransactionSynchronizationManager.getResource(bufferKey);
	}

	/**
	 * Performs the actual write to the store via the template.
	 * This can be overridden by a subclass if necessary.
	 *
	 * @param items the list of items to be persisted.
	 */
	protected void doWrite(List<? extends T> items) {
		if(! CollectionUtils.isEmpty(items)) {
			try {
				fsDataOutputStream.write(getPayloadAsBytes(items));
			} catch (IOException ioe) {
				throw new RuntimeException("Error writing to HDFS", ioe);
			}
		}
	}

	@Override
	public void open(ExecutionContext executionContext)
			throws ItemStreamException {
		try {
			Path name = null;

			name = new Path(fileName);
			fileSystem.createNewFile(name);
			this.fsDataOutputStream = fileSystem.create(name);
		} catch (IOException ioe) {
			throw new RuntimeException("Unable to open file to write to", ioe);
		}
	}

	@Override
	public void update(ExecutionContext executionContext)
			throws ItemStreamException {
		// TODO: determine the state to maintain, if any
	}

	@Override
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void write(List<? extends T> items) throws Exception {
		if(!transactionActive()) {
			doWrite(items);
			return;
		}

		List bufferedItems = getCurrentBuffer();
		bufferedItems.addAll(items);
	}

	@Override
	public void close() {
		if (fsDataOutputStream != null) {
			IOUtils.closeStream(fsDataOutputStream);
		}
	}

	/**
	 * Extracts the payload as a byte array.
	 * @param message
	 * @return the payload as byte array
	 */
	private byte[] getPayloadAsBytes(List<? extends T> items) throws IOException{
		ByteArrayOutputStream stream = new ByteArrayOutputStream();

		for (T item : items) {
			itemSerializer.serialize(item, stream);
		}

		return stream.toByteArray();
	}

	private boolean transactionActive() {
		return TransactionSynchronizationManager.isActualTransactionActive();
	}
}
