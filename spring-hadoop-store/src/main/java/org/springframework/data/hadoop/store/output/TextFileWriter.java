/*
 * Copyright 2013 the original author or authors.
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
	
	public TextFileWriter(Configuration configuration, Path basePath, CodecInfo codec, byte[] delimiter, long idleTimeout ) {
		this(configuration, basePath, codec, delimiter);
		setIdleTimeout(idleTimeout);
	}

	@Override
	public synchronized  void flush() throws IOException {
		if (streamsHolder == null) {
			streamsHolder.getStream().flush();
		}
	}

	@Override
	public synchronized void close() throws IOException {
		if (streamsHolder != null) {
			streamsHolder.close();

			renameFile(streamsHolder.getPath());

			StoreEventPublisher storeEventPublisher = getStoreEventPublisher();
			if (storeEventPublisher != null) {
				storeEventPublisher.publishEvent(new FileWrittenEvent(this, streamsHolder.getPath()));
			}

			streamsHolder = null;
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
		log.info("My context " + context + " - " + this);
		if (context.getRolloverState()) {
			log.info("after write, rollever state is true");
			close();
			context.rollStrategies();
		}


	}

	@Override
	protected void handleIdleTimeout() {
		log.info("Idle timeout detected for this writer, closing stream");
		try {
			close();
		} catch (IOException e) {
			log.error("error closing", e);
		}
		getOutputContext().rollStrategies();
	}

}
