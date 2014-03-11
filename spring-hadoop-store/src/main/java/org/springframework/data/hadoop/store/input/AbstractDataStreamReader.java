/*
 * Copyright 2014 the original author or authors.
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
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.data.hadoop.store.StoreException;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.split.Split;
import org.springframework.data.hadoop.store.support.InputContext;
import org.springframework.data.hadoop.store.support.InputStoreObjectSupport;
import org.springframework.data.hadoop.store.support.StreamsHolder;
import org.springframework.util.ClassUtils;

/**
 * A {@code AbstractDataStreamReader} is a base implementation handling
 * streams with a raw hdfs files.
 *
 * @author Janne Valkealahti
 *
 */
public class AbstractDataStreamReader extends InputStoreObjectSupport {

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
	 * Instantiates a new abstract data stream reader.
	 *
	 * @param configuration the configuration
	 * @param basePath the base path
	 * @param codec the compression codec info
	 * @param split the input split
	 */
	public AbstractDataStreamReader(Configuration configuration, Path basePath, CodecInfo codec, Split split) {
		super(configuration, basePath, codec, split);
	}

	/**
	 * Creates a holder wrapping input stream.
	 *
	 * @return the holder for streams
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected StreamsHolder<InputStream> getInput() throws IOException {
		Path inputPath = getPath();
		StreamsHolder<InputStream> holder = new StreamsHolder<InputStream>();
		final FileSystem fs = inputPath.getFileSystem(getConfiguration());
		Path p = inputPath.isAbsolute() ? inputPath : new Path(getPath(), inputPath);
		if (!fs.exists(p)) {
			throw new StoreException("Path " + p + " does not exist");
		}
		if (!isCompressed()) {
			if (getSplit() == null) {
				// no codec, no split
				InputStream input = fs.open(p);
				holder.setStream(input);
			} else {
				// no codec, with split
				FSDataInputStream input = fs.open(inputPath);
				input.seek(getSplit().getStart());
				holder.setStream(input);
			}
		} else {
			Class<?> clazz = ClassUtils.resolveClassName(getCodec().getCodecClass(), getClass().getClassLoader());
			CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(clazz,
					getConfiguration());
			Decompressor decompressor = CodecPool.getDecompressor(compressionCodec);
			if (getSplit() == null) {
				// with codec, no split
				FSDataInputStream winput = fs.open(p);
				InputStream input = compressionCodec.createInputStream(winput, decompressor);
				holder.setWrappedStream(winput);
				holder.setStream(input);
			} else {
				// with codec, with split
				long start = getSplit().getStart();
				long end = start + getSplit().getLength();
				log.info("SplitCompressionInputStream start=" + start + " end=" + end);

				FSDataInputStream winput = fs.open(p);
				SplitCompressionInputStream input = ((SplittableCompressionCodec) compressionCodec).createInputStream(
						winput, decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
				holder.setWrappedStream(winput);
				holder.setStream(input);
			}
		}
		return holder;
	}

	/**
	 * Reader helper hiding complexity of working with input streams.
	 *
	 * @param <T> the type of reader
	 * @param <V> the type of data returned by reader
	 */
	protected static abstract class ReaderHelper<T,V> {
		private final StreamsHolder<InputStream> streamsHolder;
		private final InputContext inputContext;
		private T reader;
		private Seekable seekable;
		private Split inputSplit;
		private CodecInfo codec;

		/**
		 * Instantiates a new reader helper.
		 *
		 * @param streamsHolder the streams holder
		 * @param inputContext the input context
		 * @param split the input split
		 * @param codec the compression codec info
		 */
		protected ReaderHelper(StreamsHolder<InputStream> streamsHolder, InputContext inputContext, Split split, CodecInfo codec) {
			this.streamsHolder = streamsHolder;
			this.inputContext = inputContext;
			this.inputSplit = split;
			this.codec = codec;
			if (codec != null && streamsHolder.getStream() instanceof Seekable) {
				seekable = (Seekable) streamsHolder.getStream();
			}
		}

		/**
		 * Creates the reader.
		 *
		 * @param inputStream the input stream
		 * @return the reader
		 * @throws IOException Signals that an I/O exception has occurred.
		 */
		protected abstract T createReader(InputStream inputStream) throws IOException;

		/**
		 * Do read.
		 *
		 * @param delegate the delegate reader
		 * @return the data read by a reader
		 * @throws IOException Signals that an I/O exception has occurred.
		 */
		protected abstract V doRead(T delegate) throws IOException;

		/**
		 * Inits the reader helper. This method must be called by
		 * an implementor.
		 *
		 * @throws IOException Signals that an I/O exception has occurred.
		 */
		protected void init() throws IOException {
			reader = createReader(streamsHolder.getStream());
			if (codec != null && inputSplit != null) {
			    inputContext.setStart(((SplitCompressionInputStream)streamsHolder.getStream()).getAdjustedStart());
			    inputContext.setEnd(((SplitCompressionInputStream)streamsHolder.getStream()).getAdjustedEnd());
			}
		}

		/**
		 * Processing a count of bytes read from a stream. Internally
		 * we're may use {@link Seekable} depending if it has been set.
		 *
		 * @param readSize the read size
		 * @throws IOException Signals that an I/O exception has occurred.
		 */
		protected void processReadCount(int readSize) throws IOException {
			if (seekable != null) {
				inputContext.setPosition(seekable.getPos());
			} else {
				inputContext.setPosition(inputContext.getPosition() + readSize);
			}
		}

		/**
		 * Reads a data by delegating to a reader.
		 *
		 * @return the data read by a reader
		 * @throws IOException Signals that an I/O exception has occurred.
		 */
		protected V read() throws IOException {
			return doRead(reader);
		}

		/**
		 * Gets the reader created by {@link #createReader(InputStream)}.
		 *
		 * @return the reader
		 */
		protected T getReader() {
			return reader;
		}

		/**
		 * Gets the {@link StreamsHolder} for wrapped
		 * input stream.
		 *
		 * @return the streams holder
		 */
		protected StreamsHolder<InputStream> getHolder() {
			return streamsHolder;
		}

		/**
		 * Gets the wrapped {@link InputContext}.
		 *
		 * @return the input context
		 */
		protected InputContext getContext() {
			return inputContext;
		}
	}

}
