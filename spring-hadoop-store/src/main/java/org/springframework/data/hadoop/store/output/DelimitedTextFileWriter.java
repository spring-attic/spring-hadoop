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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.support.StoreUtils;

/**
 * A {@code DelimitedTextFileWriter} is a {@code DataStoreWriter} implementation
 * able to write {@code String}s into raw hdfs files as delimited fields.
 *
 * @author Janne Valkealahti
 *
 */
public class DelimitedTextFileWriter implements DataStoreWriter<List<String>> {

	/** CSV Mode */
    public final static byte[] CSV = StoreUtils.getUTF8CsvDelimiter();

    /** TAB Mode */
    public final static byte[] TAB = StoreUtils.getUTF8TabDelimiter();

    /** Underlying text writer */
	private TextFileWriter textFileWriter;

	/** Field delimiter */
    private final String fieldDelimiter;

	/**
	 * Instantiates a new delimited text file writer.
	 *
	 * @param configuration the configuration
	 * @param basePath the base path
	 * @param codec the codec
	 */
	public DelimitedTextFileWriter(Configuration configuration, Path basePath, CodecInfo codec) {
		this(configuration, basePath, codec, CSV);
	}

	/**
	 * Instantiates a new delimited text file writer.
	 *
	 * @param configuration the configuration
	 * @param basePath the base path
	 * @param codec the codec
	 * @param fieldDelimiter the field delimiter
	 */
	public DelimitedTextFileWriter(Configuration configuration, Path basePath, CodecInfo codec, byte[] fieldDelimiter) {
		this.fieldDelimiter = new String(fieldDelimiter);
		this.textFileWriter = new TextFileWriter(configuration, basePath, codec);
	}

	/**
	 * Instantiates a new delimited text file writer.
	 *
	 * @param configuration the configuration
	 * @param basePath the base path
	 * @param codec the codec
	 * @param fieldDelimiter the field delimiter
	 * @param textDelimiter the text delimiter
	 */
	public DelimitedTextFileWriter(Configuration configuration, Path basePath, CodecInfo codec, byte[] fieldDelimiter, byte[] textDelimiter) {
		this.fieldDelimiter = new String(fieldDelimiter);
		this.textFileWriter = new TextFileWriter(configuration, basePath, codec, textDelimiter);
	}

	@Override
	public void write(final List<String> entity) throws IOException {
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i < entity.size(); i++) {
			buf.append(entity.get(i));
			if (i < (entity.size() - 1)) {
				buf.append(fieldDelimiter);
			}
		}
		textFileWriter.write(buf.toString());
	}

	@Override
	public void flush() throws IOException {
		textFileWriter.flush();
	}

	@Override
	public void close() throws IOException {
		textFileWriter.close();
	}

}
