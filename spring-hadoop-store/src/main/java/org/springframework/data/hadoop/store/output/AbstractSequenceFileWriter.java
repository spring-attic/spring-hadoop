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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.support.OutputStoreObjectSupport;
import org.springframework.data.hadoop.store.support.SequenceFileWriterHolder;
import org.springframework.util.ClassUtils;

/**
 * A {@code AbstractSequenceFileWriter} is a base implementation handling
 * writes with a {@code SequenceFile}.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractSequenceFileWriter extends OutputStoreObjectSupport {

	/**
	 * Instantiates a new abstract sequence file writer.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 */
	public AbstractSequenceFileWriter(Configuration configuration, Path basePath, CodecInfo codec) {
		super(configuration, basePath, codec);
	}

	/**
	 * Gets the output.
	 *
	 * @return the output
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@SuppressWarnings("deprecation")
	protected SequenceFileWriterHolder<Writer> getOutput() throws IOException {
		FileSystem fs = FileSystem.get(getConfiguration());

		SequenceFileWriterHolder<Writer> holder;
		Writer writer;
		CodecInfo codecInfo = getCodec();
		Path p = getResolvedPath();
		if (codecInfo == null) {
			writer = SequenceFile.createWriter(
					fs, getConfiguration(), getResolvedPath(),
					Text.class, Text.class, CompressionType.NONE, (CompressionCodec) null);
			holder = new SequenceFileWriterHolder<SequenceFile.Writer>(writer, p);
		}
		else {
			Class<?> clazz = ClassUtils.resolveClassName(codecInfo.getCodecClass(), getClass().getClassLoader());
			CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(clazz,
					getConfiguration());
			writer = SequenceFile.createWriter(fs,
					getConfiguration(), getResolvedPath(),
					Text.class, Text.class, CompressionType.RECORD, compressionCodec);
			holder = new SequenceFileWriterHolder<SequenceFile.Writer>(writer, p);
		}

		return holder;
	}

	protected long getPosition(Writer writer) throws IOException {
		if (writer != null) {
			return writer.getLength();
		} else {
			return -1;
		}
	}

}
