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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.data.hadoop.store.StoreException;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.support.StoreObjectSupport;
import org.springframework.data.hadoop.store.support.StreamsHolder;
import org.springframework.util.ClassUtils;

/**
 * A {@code AbstractDataStreamReader} is a base implementation handling
 * streams with a raw hdfs files.
 *
 * @author Janne Valkealahti
 *
 */
public class AbstractDataStreamReader extends StoreObjectSupport {

	private final static Log log = LogFactory.getLog(AbstractDataStreamReader.class);

	/**
	 * Instantiates a new abstract data stream reader.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 */
	public AbstractDataStreamReader(Configuration configuration, Path basePath, CodecInfo codec) {
		super(configuration, basePath, codec);
	}

	/**
	 * Gets the input.
	 *
	 * @param inputPath the input path
	 * @return the input
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected StreamsHolder<InputStream> getInput(Path inputPath) throws IOException {
		log.info("Creating new InputStream");
		StreamsHolder<InputStream> holder = new StreamsHolder<InputStream>();
		final FileSystem fs = getPath().getFileSystem(getConfiguration());
		Path p = inputPath.isAbsolute() ? inputPath : new Path(getPath(), inputPath);
		if (!fs.exists(p)) {
			throw new StoreException("Path " + p + " does not exist");
		}
		if (!isCompressed()) {
			InputStream input = fs.open(p);
			holder.setStream(input);
		}
		else {
			// TODO: will isCompressed() really guard for npe against getCodec()
			Class<?> clazz = ClassUtils.resolveClassName(getCodec().getCodecClass(), getClass().getClassLoader());
			CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(clazz,
					getConfiguration());
			Decompressor decompressor = CodecPool.getDecompressor(compressionCodec);
			FSDataInputStream winput = fs.open(p);
			InputStream input = compressionCodec.createInputStream(winput, decompressor);
			holder.setWrappedStream(winput);
			holder.setStream(input);
		}
		return holder;
	}

}
