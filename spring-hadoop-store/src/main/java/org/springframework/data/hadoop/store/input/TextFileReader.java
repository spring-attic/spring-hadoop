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

package org.springframework.data.hadoop.store.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.springframework.data.hadoop.store.DataStoreReader;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.support.StreamsHolder;

/**
 * A {@code TextFileReader} is a {@code DataStoreReader} implementation
 * able to read {@code String}s from a raw hdfs files.
 *
 * @author Janne Valkealahti
 *
 */
public class TextFileReader extends AbstractDataStreamReader implements DataStoreReader<String> {

	private StreamsHolder<InputStream> streamsHolder;

	private LineReader lineReader;

	private final byte[] delimiter;

	/**
	 * Instantiates a new text file reader.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 */
	public TextFileReader(Configuration configuration, Path basePath, CodecInfo codec) {
		this(configuration, basePath, codec, null);
	}

	/**
	 * Instantiates a new text file reader.
	 *
	 * @param configuration the configuration
	 * @param basePath the base path
	 * @param codec the codec
	 * @param delimiter the delimiter
	 */
	public TextFileReader(Configuration configuration, Path basePath, CodecInfo codec, byte[] delimiter) {
		super(configuration, basePath, codec);
		this.delimiter = delimiter;
	}

	@Override
	public void close() throws IOException {
		if (lineReader != null) {
			lineReader.close();
			lineReader = null;
		}
		if (streamsHolder != null) {
			streamsHolder.close();
		}
	}

	@Override
	public String read() throws IOException {
		if (streamsHolder == null) {
			streamsHolder = getInput(getPath());
			lineReader = new LineReader(streamsHolder.getStream(), delimiter);
		}
		Text text = new Text();
		lineReader.readLine(text);
		byte[] value = text.getBytes();
		return value != null && value.length > 0 ? new String(value) : null;
	}

}
