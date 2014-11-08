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
import org.springframework.data.hadoop.store.StoreException;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.support.OutputStoreObjectSupport;
import org.springframework.data.hadoop.store.support.StreamsHolder;
import org.springframework.util.ClassUtils;

/**
 * A {@code AbstractDataStreamWriter} is a base implementation handling streams
 * with a raw hdfs files.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractDataStreamWriter extends OutputStoreObjectSupport {

	private final static Log log = LogFactory.getLog(AbstractDataStreamWriter.class);

	public final static int DEFAULT_MAX_OPEN_ATTEMPTS = 10;

	private int maxOpenAttempts = DEFAULT_MAX_OPEN_ATTEMPTS;

	/**
	 * We use this jvm level lock in this class to guard against one
	 * scenario. When we try to create a stream a check is first done
	 * if path exists and then we create a stream. This sync lock would
	 * not make sense on a global level because exist()/create() is never
	 * atomic but we want to do this within a jvm. Some distros have a little
	 * different functionality in cases when same leaseholder is trying to
	 * re-create a stream with already open file. i.e. in cdh this operation
	 * on default takes 5 minutes while vanilla hadoop fails immediately.
	 * We minimise this risk within a jvm in a way that same leaseholder
	 * would not try to use same path to create a stream. In different jvm's
	 * error handling is different because of different leaseholders.
	 */
	private final static Object lock = new Object();

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
	 * Sets the max open attempts trying to find a suitable path for output
	 * stream. Only positive values are allowed and any attempt to set this to
	 * less than 1 will automatically reset value to exactly 1.
	 *
	 * @param maxOpenAttempts the new max open attempts
	 */
	public void setMaxOpenAttempts(int maxOpenAttempts) {
		this.maxOpenAttempts = maxOpenAttempts < 1 ? 1 : maxOpenAttempts;
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

		// Using maxOpenAttempts try to resolve path and open
		// an output stream and automatically rolling strategies
		// to find a next candidate. Effectively if maxOpenAttempts
		// is set to roughly same count as expected number of writers
		// and strategy init is accurate enough to find a good starting
		// position for naming, we should always get a next available
		// path and its stream.
		Path p = null;
		FSDataOutputStream wout = null;
		int openAttempt = 0;

		do {
			boolean rollStrategies = false;
			try {
				p = getResolvedPath();
				synchronized (lock) {
					boolean exists = fs.exists(p);
					if (isAppendable() && exists) {
						wout = fs.append(p);
						break;
					} else if (!isOverwrite() && exists) {
						// don't rely on error to roll.
						// check notes for lock object.
						rollStrategies = true;
					} else {
						wout = fs.create(p, isOverwrite());
						break;
					}
				}
			} catch (Exception e) {
				rollStrategies = true;
			}

			if (rollStrategies) {
				getOutputContext().rollStrategies();
			}

		} while (++openAttempt < maxOpenAttempts);

		if (wout == null) {
			throw new StoreException("We've reached maxOpenAttempts=" + maxOpenAttempts
					+ " to find suitable output path. Last path tried was path=[" + p + "]");
		}

		log.info("Creating output for path " + p);
		holder.setPath(p);

		if (!isCompressed()) {
			holder.setStream(wout);
		} else {
			// TODO: will isCompressed() really guard for npe against getCodec()
			Class<?> clazz = ClassUtils.resolveClassName(getCodec().getCodecClass(), getClass().getClassLoader());
			CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(clazz,
					getConfiguration());
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
