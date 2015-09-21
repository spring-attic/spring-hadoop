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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.event.FileWrittenEvent;
import org.springframework.data.hadoop.store.event.StoreEventPublisher;
import org.springframework.data.hadoop.store.support.OutputContext;
import org.springframework.data.hadoop.store.support.SequenceFileWriterHolder;

/**
 * A {@code TextSequenceFileWriter} is a {@code DataStoreWriter} implementation
 * able to write {@code String}s into a {@code SequenceFile}.
 *
 * @author Janne Valkealahti
 *
 */
public class TextSequenceFileWriter extends AbstractSequenceFileWriter implements DataStoreWriter<String> {

	private final static Log log = LogFactory.getLog(TextSequenceFileWriter.class);

	private SequenceFileWriterHolder<Writer> holder;

	private final static Text NULL_KEY = new Text(new byte[0]);

	/**
	 * Instantiates a new text sequence file writer.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 */
	public TextSequenceFileWriter(Configuration configuration, Path basePath, CodecInfo codec) {
		super(configuration, basePath, codec);
	}

	@Override
	public void flush() throws IOException {
    	// nothing to do
	}

    public synchronized  void hflush() throws IOException {
    	// nothing to do
    }

	@Override
	public synchronized void close() throws IOException {
		if (holder != null) {
			holder.close();

			Path path = renameFile(holder.getPath());

			StoreEventPublisher storeEventPublisher = getStoreEventPublisher();
			if (storeEventPublisher != null) {
				storeEventPublisher.publishEvent(new FileWrittenEvent(this, path));
			}

			holder = null;
		}
	}

	@Override
	public synchronized void write(String entity) throws IOException {
		if (holder == null) {
			holder = getOutput();
		}
		holder.getWriter().append(NULL_KEY, new Text(entity.getBytes()));

		setWritePosition(getPosition(holder.getWriter()));

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
            if(isAppendable()){
                log.info("Timeout detected for this writer, flushing stream");
                hflush();
            }
            else{
                log.info("Timeout detected for this writer, closing stream");
                close();
            }
        } catch (IOException e) {
            log.error("Error closing", e);
        }
		getOutputContext().rollStrategies();
	}

}
