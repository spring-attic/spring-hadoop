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
package org.springframework.data.hadoop.store.input;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.DataStoreReader;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.split.Split;
import org.springframework.data.hadoop.store.support.StoreUtils;
import org.springframework.util.StringUtils;

/**
 * A {@code DelimitedTextFileReader} is a {@code DataStoreReader} implementation
 * able to read {@code String}s from a raw hdfs files as delimited fields.
 *
 * @author Janne Valkealahti
 *
 */
public class DelimitedTextFileReader implements DataStoreReader<List<String>> {

	/** CSV Mode */
    public final static byte[] CSV = StoreUtils.getUTF8CsvDelimiter();

    /** TAB Mode */
    public final static byte[] TAB = StoreUtils.getUTF8TabDelimiter();

    /** Underlying text writer */
	private TextFileReader textFileReader;

	/** Field delimiter */
    private final String fieldDelimiter;

	/**
	 * Instantiates a new delimited text file reader.
	 *
	 * @param configuration the configuration
	 * @param basePath the base path
	 * @param codec the codec
	 */
	public DelimitedTextFileReader(Configuration configuration, Path basePath, CodecInfo codec) {
		this(configuration, basePath, codec, null, CSV, null);
	}

	/**
	 * Instantiates a new delimited text file reader.
	 *
	 * @param configuration the configuration
	 * @param basePath the base path
	 * @param codec the codec
	 * @param fieldDelimiter the field delimiter
	 */
	public DelimitedTextFileReader(Configuration configuration, Path basePath, CodecInfo codec, byte[] fieldDelimiter) {
		this(configuration, basePath, codec, null, fieldDelimiter, null);
	}

	/**
	 * Instantiates a new delimited text file reader.
	 *
	 * @param configuration the configuration
	 * @param basePath the base path
	 * @param codec the codec
	 * @param inputSplit the input split
	 * @param fieldDelimiter the field delimiter
	 * @param textDelimiter the text delimiter
	 */
	public DelimitedTextFileReader(Configuration configuration, Path basePath, CodecInfo codec, Split inputSplit, byte[] fieldDelimiter, byte[] textDelimiter) {
		this.fieldDelimiter = new String(fieldDelimiter);
		this.textFileReader = new TextFileReader(configuration, basePath, codec, inputSplit, textDelimiter);
	}

	@Override
	public List<String> read() throws IOException {
		String line = textFileReader.read();
		return StringUtils.hasText(line) ? Arrays.asList(line.split(fieldDelimiter)) : null;
	}

	@Override
	public void close() throws IOException {
		textFileReader.close();
	}

}
