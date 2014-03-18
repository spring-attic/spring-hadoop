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
package org.springframework.data.hadoop.store.support;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;
import org.springframework.util.Assert;

/**
 * Holder object for streams. Mostly concept of a wrapped streams are used with
 * a compressed streams in a hadoop where some information still needs to
 * be accessed from an underlying stream.
 *
 * @param <T> a type of a stream
 * @author Janne Valkealahti
 *
 */
public class StreamsHolder<T extends Closeable> implements Closeable {

	private T stream;

	private T wrappedStream;

	private Path path;

	/**
	 * Instantiates a new streams holder.
	 */
	public StreamsHolder() {
	}

	/**
	 * Instantiates a new streams holder.
	 *
	 * @param stream the stream
	 * @param wrappedStream the wrapped stream
	 */
	public StreamsHolder(T stream, T wrappedStream) {
		this(stream, wrappedStream, null);
	}

	/**
	 * Instantiates a new streams holder.
	 *
	 * @param stream the stream
	 * @param wrappedStream the wrapped stream
	 * @param path the path
	 */
	public StreamsHolder(T stream, T wrappedStream, Path path) {
		Assert.notNull(stream, "Main stream should not be null");
		this.stream = stream;
		this.wrappedStream = wrappedStream;
		this.path = path;
	}

	/**
	 * Close both streams in this holder. Possible {@code IOException} by closing wrapped stream is not thrown.
	 *
	 * @see InputStream#close()
	 * @see OutputStream#close()
	 */
	@Override
	public void close() throws IOException {
		if (stream != null) {
			stream.close();
		}
		if (wrappedStream != null) {
			try {
				wrappedStream.close();
			}
			catch (IOException e) {
				// try to close but eat IOException because it was
				// already closed by the main stream or something
				// else happened what we should not care about
			}
		}
	}

	/**
	 * Gets the stream.
	 *
	 * @return the stream
	 */
	public T getStream() {
		return stream;
	}

	/**
	 * Sets the stream.
	 *
	 * @param stream the new stream
	 */
	public void setStream(T stream) {
		this.stream = stream;
	}

	/**
	 * Gets the wrapped stream.
	 *
	 * @return the wrapped stream
	 */
	public T getWrappedStream() {
		return wrappedStream;
	}

	/**
	 * Sets the wrapped stream.
	 *
	 * @param wrappedStream the new wrapped stream
	 */
	public void setWrappedStream(T wrappedStream) {
		this.wrappedStream = wrappedStream;
	}

	/**
	 * Gets the path.
	 *
	 * @return the path
	 */
	public Path getPath() {
		return path;
	}

	/**
	 * Sets the path.
	 *
	 * @param path the new path
	 */
	public void setPath(Path path) {
		this.path = path;
	}

}
