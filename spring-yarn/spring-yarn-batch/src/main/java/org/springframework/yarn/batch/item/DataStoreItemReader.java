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
package org.springframework.yarn.batch.item;

import java.io.IOException;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.ReaderNotOpenException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.data.hadoop.store.DataStoreReader;
import org.springframework.util.Assert;

/**
 * Implementation of {@link org.springframework.batch.item.ItemReader} able
 * to read items from HDFS file splits.
 *
 * @author Janne Valkealahti
 *
 * @param <T> the type of object returned as item read
 */
public class DataStoreItemReader<T> implements ItemStreamReader<T> {

	public static final String READ_POSITION = "read.position";

	private long position = 0;

	private DataStoreReader<T> dataStoreReader;

	/** Mapper for data */
	private LineDataMapper<T> lineDataMapper;

	/** State flag for saving into execution context */
	private boolean saveState = true;

	/**
	 * Instantiates a new data store item reader.
	 */
	public DataStoreItemReader() {
	}

	/**
	 * Instantiates a new data store item reader.
	 *
	 * @param dataStoreReader the data store reader
	 */
	public DataStoreItemReader(DataStoreReader<T> dataStoreReader) {
		this.dataStoreReader = dataStoreReader;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		Assert.notNull(dataStoreReader, "DataStoreReader must be set");
		Assert.notNull(lineDataMapper, "LineDataMapper must be set");
		if (saveState) {
			restorePosition(executionContext.getLong(READ_POSITION, -1));
		}
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		if (saveState) {
			Assert.notNull(executionContext, "ExecutionContext must not be null");
			executionContext.putLong(READ_POSITION, position);
		}
	}

	@Override
	public void close() throws ItemStreamException {
		if (dataStoreReader != null) {
			try {
				dataStoreReader.close();
			} catch (IOException e) {
				throw new ItemStreamException("Error while closing item reader", e);
			} finally {
				dataStoreReader = null;
			}
		}
	}

	@Override
	public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		T line = readStore();
		if (line == null) {
			return null;
		} else {
			position++;
		}
		return lineDataMapper.mapLine((String)line);
	}

	/**
	 * Sets the line data mapper.
	 *
	 * @param lineDataMapper the new line data mapper
	 */
	public void setLineDataMapper(LineDataMapper<T> lineDataMapper) {
		this.lineDataMapper = lineDataMapper;
	}

	/**
	 * Sets the data store reader.
	 *
	 * @param dataStoreReader the new data store reader
	 */
	public void setDataStoreReader(DataStoreReader<T> dataStoreReader) {
		this.dataStoreReader = dataStoreReader;
	}

	/**
	 * Set the flag that determines whether to save internal data for
	 * {@link ExecutionContext}. Only switch this to false if you don't want to
	 * save any state from this stream, and you don't need it to be restartable.
	 * Always set it to false if the reader is being used in a concurrent
	 * environment.
	 *
	 * @param saveState flag value (default true).
	 */
	public void setSaveState(boolean saveState) {
		this.saveState = saveState;
	}

	/**
	 * The flag that determines whether to save internal state for restarts.
	 * @return true if the flag was set
	 */
	public boolean isSaveState() {
		return saveState;
	}

	/**
	 * Read line from input and update read position.
	 *
	 * @return data or {@code NULL} if end of stream
	 */
	private T readStore() {
		if (dataStoreReader == null) {
			throw new ReaderNotOpenException("Reader must be open before it can be read.");
		}
		try {
			return dataStoreReader.read();
		} catch (IOException e) {
			throw new NonTransientResourceException("Unable to read from resource: [" + dataStoreReader + "]", e);
		}
	}

	/**
	 * Restores a position if it's possible.
	 *
	 * @param toPosition the position to restore
	 */
	private void restorePosition(long toPosition) throws ItemStreamException {
		if (toPosition < 0) {
			return;
		}
		while (readStore() != null) {
			if (!(++position < toPosition)) {
				break;
			}
		}
		if (position < toPosition) {
			throw new ItemStreamException("Expected to restore to position " + toPosition
					+ " but was only able to read to position " + position);
		}
	}

}
