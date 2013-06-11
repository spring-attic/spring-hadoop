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
package org.springframework.yarn.batch.item;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.ReaderNotOpenException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

/**
 * Implementation of {@link org.springframework.batch.item.ItemReader} able
 * to read items from HDFS file splits.
 *
 * @author Janne Valkealahti
 *
 * @param <T> the type of object returned as item read
 */
public class HdfsFileSplitItemReader<T> implements ResourceAwareItemReaderItemStream<T> {

	private static final String READ_POSITION = "read.position";

	/** Resource to read */
	private Resource resource;

	/** Start position for the file */
	private long splitStart;

	/** Length of the split for file read */
	private long splitLength;

	/** Current input stream read position */
	private long position;

	/** Custom line reader */
	private LineReader lineReader;

	/** Exposed input stream for seek */
	private FSDataInputStream fsInputStream;

	/** Read buffer */
	private Text buffer = new Text();

	/** Mapper for data */
	private LineDataMapper<T> lineDataMapper;

	/** State flag for saving into execution context */
	private boolean saveState = true;

	/**
	 * Instantiates a new hdfs file split item reader.
	 */
	public HdfsFileSplitItemReader() {
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		Assert.notNull(resource, "Input resource must be set");
		Assert.notNull(lineDataMapper, "LineDataMapper must be set");

		try {
			InputStream inputStream = resource.getInputStream();
			fsInputStream = (FSDataInputStream)inputStream;
			fsInputStream.seek(splitStart);
			lineReader = new LineReader(fsInputStream);
		} catch (Exception e) {
			throw new ItemStreamException("Failed to initialize the reader", e);
		}

		position = splitStart;
		if (splitStart != 0) {
			readLine();
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
		if (fsInputStream != null) {
			try {
				fsInputStream.close();
			} catch (IOException e) {
				throw new ItemStreamException("Error while closing item reader", e);
			}
		}
	}

	@Override
	public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		if (position > (splitStart+splitLength)) {
			return null;
		}
		String line = readLine();

		if (line == null) {
			return null;
		}

		return lineDataMapper.mapLine(line);
	}

	@Override
	public void setResource(Resource resource) {
		this.resource = resource;
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
	 * Sets the split start.
	 *
	 * @param splitStart the new split start
	 */
	public void setSplitStart(long splitStart) {
		this.splitStart = splitStart;
	}

	/**
	 * Sets the split length.
	 *
	 * @param splitLength the new split length
	 */
	public void setSplitLength(long splitLength) {
		this.splitLength = splitLength;
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
	private String readLine() {
		if (lineReader == null) {
			throw new ReaderNotOpenException("Reader must be open before it can be read.");
		}
		buffer.clear();
		try {
			int consumed = lineReader.readLine(buffer);
			position += consumed;
			if (consumed == 0) {
				return null;
			}
		}
		catch (IOException e) {
			throw new NonTransientResourceException("Unable to read from resource: [" + resource + "]", e);
		}
		return buffer.toString();
	}

}
