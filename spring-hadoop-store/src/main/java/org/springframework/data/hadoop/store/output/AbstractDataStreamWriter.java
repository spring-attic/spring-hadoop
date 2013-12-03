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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.support.OutputStoreObjectSupport;
import org.springframework.data.hadoop.store.support.StreamsHolder;
import org.springframework.util.ClassUtils;

/**
 * A {@code AbstractDataStreamWriter} is a base implementation handling
 * streams with a raw hdfs files.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractDataStreamWriter extends OutputStoreObjectSupport {

	private final static Log log = LogFactory.getLog(AbstractDataStreamWriter.class);

	/**
	 * Instantiates a new abstract data stream writer.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 */
	public AbstractDataStreamWriter(Configuration configuration, Path basePath, CodecInfo codec) {
		super(configuration, basePath, codec);
	}

	/**
	 * Gets the output.
	 *
	 * @return the output
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected StreamsHolder<OutputStream> getOutput() throws IOException {
		StreamsHolder<OutputStream> holder = new StreamsHolder<OutputStream>();
		FileSystem fs = FileSystem.get(getConfiguration());
		Path p = getResolvedPath();
		log.info("Creating output for path " + p);
		holder.setPath(p);
		if (!isCompressed()) {
			OutputStream out = fs.create(p);
			holder.setStream(out);
		} else {
			// TODO: will isCompressed() really guard for npe against getCodec()
			Class<?> clazz = ClassUtils.resolveClassName(getCodec().getCodecClass(), getClass().getClassLoader());
			CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(clazz,
					getConfiguration());
			FSDataOutputStream wout = fs.create(p);
			OutputStream out = compressionCodec.createOutputStream(wout);
			holder.setWrappedStream(wout);
			holder.setStream(out);
		}
		return holder;
	}

	/**
	 * Gets the current stream writing position.
	 *
	 * @param holder the holder for output streams
	 * @return the position
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected long getPosition(StreamsHolder<OutputStream> holder) throws IOException {
		if (holder != null) {
			OutputStream out = holder.getStream();
			OutputStream wout = holder.getWrappedStream();
			if (out instanceof FSDataOutputStream) {
				return ((FSDataOutputStream) out).getPos();
			} else if (wout instanceof FSDataOutputStream) {
				return ((FSDataOutputStream) wout).getPos();
			} else {
				return -1;
			}
		} else {
			return -1;
		}
	}

}
