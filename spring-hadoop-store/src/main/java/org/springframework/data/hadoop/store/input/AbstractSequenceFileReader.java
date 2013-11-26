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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.support.StoreObjectSupport;

/**
 * A {@code AbstractSequenceFileReader} is a base implementation handling
 * reads with a {@code SequenceFile}.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractSequenceFileReader extends StoreObjectSupport {

	/**
	 * Instantiates a new abstract sequence file reader.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 */
	public AbstractSequenceFileReader(Configuration configuration, Path basePath, CodecInfo codec) {
		super(configuration, basePath, codec);
	}

	@SuppressWarnings("deprecation")
	protected Reader getInput() throws IOException {
		FileSystem fileSystem = getPath().getFileSystem(getConfiguration());
		return new SequenceFile.Reader(fileSystem, getPath(), getConfiguration());
	}
}
