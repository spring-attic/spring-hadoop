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
import org.springframework.data.hadoop.store.split.Split;

/**
 * A {@code TextFileReader} is a {@code DataStoreReader} implementation
 * able to read {@code String}s from a raw hdfs files.
 *
 * @author Janne Valkealahti
 *
 */
public class TextFileReader extends AbstractDataStreamReader implements DataStoreReader<String> {

	private ReaderHelper<LineReader, byte[]> readerHelper;

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
	 * @param split the input split
	 */
	public TextFileReader(Configuration configuration, Path basePath, CodecInfo codec, Split split) {
		this(configuration, basePath, codec, split, null);
	}

	/**
	 * Instantiates a new text file reader.
	 *
	 * @param configuration the configuration
	 * @param basePath the base path
	 * @param codec the codec
	 * @param split the input split
	 * @param delimiter the delimiter
	 */
	public TextFileReader(Configuration configuration, Path basePath, CodecInfo codec, Split split, byte[] delimiter) {
		super(configuration, basePath, codec, split);
		this.delimiter = delimiter;
	}

	@Override
	public void close() throws IOException {
		if (readerHelper != null) {
			if (readerHelper.getReader() != null) {
				readerHelper.getReader().close();
			}
			if (readerHelper.getHolder() != null) {
				readerHelper.getHolder().close();
			}
			readerHelper = null;
		}
	}

	@Override
	public String read() throws IOException  {
		if (readerHelper == null) {
			readerHelper = new ReaderHelper<LineReader, byte[]>(getInput(), getInputContext(), getSplit(), getCodec()) {
				@Override
				protected LineReader createReader(InputStream inputStream) throws IOException {
					LineReader lineReader = new LineReader(inputStream, delimiter);
					if (getContext().getStart() > 0) {
						processReadCount(lineReader.readLine(new Text()));
					}
					return lineReader;
				}

				@Override
				protected byte[] doRead(LineReader delegate) throws IOException {
					Text text = new Text();
					if (!getInputContext().isEndReached()) {
						processReadCount(delegate.readLine(text));
					}
					return text.getBytes();
				}
			};
			readerHelper.init();
		}
		byte[] value = readerHelper.read();
		return value != null && value.length > 0 ? new String(value) : null;
	}

}
