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
import org.springframework.data.hadoop.store.support.StreamsHolder;

/**
 * A {@code OutputStreamWriter} is a {@code DataStoreWriter} implementation
 * able to write {@code byte[]}s into raw hdfs files.
 *
 * @author Janne Valkealahti
 *
 */
public class OutputStreamWriter extends AbstractDataStreamWriter implements DataStoreWriter<byte[]> {

	private final static Log log = LogFactory.getLog(OutputStreamWriter.class);

	private StreamsHolder<OutputStream> streamsHolder;

	/**
	 * Instantiates a new output stream writer.
	 *
	 * @param configuration the configuration
	 * @param basePath the base path
	 * @param codec the codec
	 */
	public OutputStreamWriter(Configuration configuration, Path basePath, CodecInfo codec) {
		super(configuration, basePath, codec);
	}

	@Override
	public void flush() throws IOException {
		if (streamsHolder != null) {
			streamsHolder.getStream().flush();
		}
	}

    public synchronized  void hflush() throws IOException {
        if (streamsHolder != null) {
            ((Syncable)streamsHolder.getStream()).hflush();
        }
    }

	@Override
	public synchronized void close() throws IOException {
		if (streamsHolder != null) {
			streamsHolder.close();

			Path path = renameFile(streamsHolder.getPath());

			StoreEventPublisher storeEventPublisher = getStoreEventPublisher();
			if (storeEventPublisher != null) {
				storeEventPublisher.publishEvent(new FileWrittenEvent(this, path));
			}

			streamsHolder = null;
		}
	}

	@Override
	public synchronized void write(byte[] entity) throws IOException {
		if (streamsHolder == null) {
			streamsHolder = getOutput();
		}
		OutputStream out = streamsHolder.getStream();
		out.write(entity);

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
			if (isAppendable()) {
				log.info("Timeout detected for this writer, flushing stream");
				hflush();
			} else {
				log.info("Timeout detected for this writer, closing stream");
				close();
			}
		} catch (IOException e) {
			log.error("Error closing", e);
		}
		getOutputContext().rollStrategies();
	}

}
