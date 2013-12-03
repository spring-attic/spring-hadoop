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
package org.springframework.data.hadoop.store.support;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

/**
 * Holder object for {@code SequenceFile} writer.
 *
 * @param <T> a type of a writer
 * @author Janne Valkealahti
 *
 */
public class SequenceFileWriterHolder<T extends Closeable> implements Closeable {

	private T writer;

	private Path path;

	/**
	 * Instantiates a new sequence file writer holder.
	 */
	public SequenceFileWriterHolder() {}

	/**
	 * Instantiates a new sequence file writer holder.
	 *
	 * @param writer the writer
	 */
	public SequenceFileWriterHolder(T writer) {
		this(writer, null);
	}

	/**
	 * Instantiates a new sequence file writer holder.
	 *
	 * @param writer the writer
	 * @param path the path
	 */
	public SequenceFileWriterHolder(T writer, Path path) {
		this.writer = writer;
		this.path = path;
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}

	/**
	 * Gets the writer.
	 *
	 * @return the writer
	 */
	public T getWriter() {
		return writer;
	}

	/**
	 * Gets the path.
	 *
	 * @return the path
	 */
	public Path getPath() {
		return path;
	}

}
