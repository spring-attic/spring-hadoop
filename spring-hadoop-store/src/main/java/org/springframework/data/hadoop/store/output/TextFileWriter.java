/*
 * Copyright 2013-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.store.output;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.event.FileWrittenEvent;
import org.springframework.data.hadoop.store.event.StoreEventPublisher;
import org.springframework.data.hadoop.store.support.OutputContext;
import org.springframework.data.hadoop.store.support.StoreUtils;
import org.springframework.data.hadoop.store.support.StreamsHolder;

/**
 * A {@code TextFileWriter} is a {@code DataStoreWriter} implementation
 * able to write {@code String}s into raw hdfs files.
 *
 * @author Janne Valkealahti
 * @author Rodrigo Meneses
 *
 */
public class TextFileWriter extends AbstractDataStreamWriter implements DataStoreWriter<String> {

	private final static Log log = LogFactory.getLog(TextFileWriter.class);

	private StreamsHolder<OutputStream> streamsHolder;

	private final byte[] delimiter;

	/**
	 * Instantiates a new text file writer.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 */
	public TextFileWriter(Configuration configuration, Path basePath, CodecInfo codec) {
		this(configuration, basePath, codec, StoreUtils.getUTF8DefaultDelimiter());

	}

	/**
	 * Instantiates a new text file writer.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 * @param delimiter the delimiter
	 */
	public TextFileWriter(Configuration configuration, Path basePath, CodecInfo codec, byte[] delimiter) {
		super(configuration, basePath, codec);
		this.delimiter = delimiter;
	}

	/**
	 * Instantiates a new text file writer.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 * @param delimiter the delimiter
	 * @param idleTimeout the idle timeout
	 */
	public TextFileWriter(Configuration configuration, Path basePath, CodecInfo codec, byte[] delimiter, long idleTimeout ) {
		this(configuration, basePath, codec, delimiter);
		setIdleTimeout(idleTimeout);
	}

	@Override
	public synchronized void flush() throws IOException {
		if (streamsHolder != null) {
			OutputStream stream = streamsHolder.getStream();
			stream.flush();
			if ((isAppendable() || isSyncable()) && stream instanceof Syncable) {
				((Syncable)stream).hflush();
			}
		}
	}

	@Override
	public synchronized void close() throws IOException {
		if (streamsHolder != null) {

			// we store the possible error and rethrow it
			// later so that we can null holder for further
			// operations not to fail
			IOException rethrow = null;
			try {
				streamsHolder.close();

				Path path = renameFile(streamsHolder.getPath());

				StoreEventPublisher storeEventPublisher = getStoreEventPublisher();
				if (storeEventPublisher != null) {
					storeEventPublisher.publishEvent(new FileWrittenEvent(this, path));
				}
			} catch (IOException e) {
				rethrow = e;
				log.error("Error in close", e);
			} finally {
				streamsHolder = null;
			}
			if (rethrow != null) {
				throw rethrow;
			}
		}
	}

	@Override
	public synchronized void write(String entity) throws IOException {
		if (streamsHolder == null) {
			streamsHolder = getOutput();
		}
		OutputStream out = streamsHolder.getStream();
		out.write(entity.getBytes());
		out.write(delimiter);
		setWritePosition(getPosition(streamsHolder));

		OutputContext context = getOutputContext();
		if (context.getRolloverState()) {
			log.info("After write, rollover state is true");
			close();
			context.rollStrategies();
		}
	}

	@Override
	protected void handleTimeout() {
		try {
			log.info("Timeout detected for this writer=[" + this +  "], closing stream");
			flush();
			close();
		} catch (IOException e) {
			log.error("Error closing", e);
		}
		getOutputContext().rollStrategies();
	}

	@Override
	protected void flushTimeout() {
		try {
			flush();
		} catch (IOException e) {
			log.error("Error flushing stream", e);
		}
	}

}
