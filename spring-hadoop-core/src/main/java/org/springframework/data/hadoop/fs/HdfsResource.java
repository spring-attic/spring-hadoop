/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.hadoop.fs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.springframework.core.io.ContextResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * {@link Resource} implementation over HDFS {@link Path}s.
 *
 * @author Costin Leau
 * @author Janne Valkealahti
 *
 */
class HdfsResource implements ContextResource, WritableResource {

	private final String location;
	private final Path path;
	private final FileSystem fs;
	private boolean exists;
	private final FileStatus status;
	private final CompressionCodecFactory codecsFactory;

	/**
	 * Instantiates a new hdfs resource.
	 *
	 * @param location the location
	 * @param fs the fs
	 * @param codecsFactory the codecs factory
	 */
	HdfsResource(String location, FileSystem fs, CompressionCodecFactory codecsFactory) {
		this(location, null, fs, codecsFactory);
	}

	/**
	 * Instantiates a new hdfs resource.
	 *
	 * @param parent the parent
	 * @param child the child
	 * @param fs the fs
	 * @param codecsFactory the codecs factory
	 */
	HdfsResource(String parent, String child, FileSystem fs, CompressionCodecFactory codecsFactory) {
		this(StringUtils.hasText(child) ? new Path(new Path(URI.create(parent)), new Path(URI.create(child)))
			: new Path(URI.create(parent)), fs, codecsFactory);
	}

	/**
	 * Instantiates a new hdfs resource.
	 *
	 * @param parent the parent
	 * @param child the child
	 * @param fs the fs
	 * @param codecsFactory the codecs factory
	 */
	HdfsResource(Path parent, Path child, FileSystem fs, CompressionCodecFactory codecsFactory) {
		this(new Path(parent, child), fs, codecsFactory);
	}

	/**
	 * Instantiates a new hdfs resource.
	 *
	 * @param path the path
	 * @param fs the fs
	 * @param codecsFactory the codecs factory
	 */
	@SuppressWarnings("deprecation")
	HdfsResource(Path path, FileSystem fs, CompressionCodecFactory codecsFactory) {
		Assert.notNull(path, "a valid path is required");
		Assert.notNull(fs, "non null file system required");

		this.location = path.toString();
		this.fs = fs;
		this.path = path.makeQualified(fs);

		boolean exists = false;

		try {
			exists = fs.exists(path);
		} catch (Exception ex) {
		}
		this.exists = exists;

		FileStatus status = null;
		try {
			status = fs.getFileStatus(path);
		} catch (Exception ex) {
		}
		this.status = status;
		this.codecsFactory = codecsFactory;
	}


	@Override
	public long contentLength() throws IOException {
		if (exists) {
			if (status != null) {
				return status.getLen();
			}
		}
		throw new IOException("Cannot access the status for " + getDescription());
	}

	@Override
	public Resource createRelative(String relativePath) throws IOException {
		return new HdfsResource(location, relativePath, fs, codecsFactory);
	}

	@Override
	public boolean exists() {
		return exists;
	}

	@Override
	public String getDescription() {
		return "HDFS Resource for [" + location + "]";
	}

	@Override
	public File getFile() throws IOException {
		// check for out-of-the-box localFS
		if (fs instanceof RawLocalFileSystem) {
			return ((RawLocalFileSystem) fs).pathToFile(path);
		}

		if (fs instanceof LocalFileSystem) {
			return ((LocalFileSystem) fs).pathToFile(path);
		}

		throw new UnsupportedOperationException("Cannot resolve File object for " + getDescription());
	}

	@Override
	public String getFilename() {
		return path.getName();
	}

	@Override
	public URI getURI() throws IOException {
		return path.toUri();
	}

	@Override
	public URL getURL() throws IOException {
		return path.toUri().toURL();
	}

	@Override
	public boolean isOpen() {
		return (exists ? true : false);
	}

	@Override
	public boolean isReadable() {
		return (exists ? true : false);
	}

	@Override
	public long lastModified() throws IOException {
		if (exists && status != null) {
			return status.getModificationTime();
		}
		throw new IOException("Cannot get timestamp for " + getDescription());
	}

	@Override
	public InputStream getInputStream() throws IOException {
		if (exists) {
			InputStream stream = fs.open(path);

			if (codecsFactory != null) {
				CompressionCodec codec = codecsFactory.getCodec(path);
				if (codec != null) {
					// the pool is not used since the returned inputstream needs to be decorated
					// to return the decompressor on close which can mask the actual stream
					// it's also unclear whether the pool is actually useful or not
					// Decompressor decompressor = CodecPool.getDecompressor(codec);
					// stream = (decompressor != null ? codec.createInputStream(stream, decompressor) : codec.createInputStream(stream));
					stream = codec.createInputStream(stream);
				}
			}

			return stream;
		}
		throw new IOException("Cannot open stream for " + getDescription());
	}

	/**
	 * This implementation returns the description of this resource.
	 * @see #getDescription()
	 */
	@Override
	public String toString() {
		return getDescription();
	}

	/**
	 * This implementation compares description strings.
	 * @see #getDescription()
	 */
	@Override
	public boolean equals(Object obj) {
		return (obj == this || (obj instanceof Resource && ((Resource) obj).getDescription().equals(getDescription())));
	}

	/**
	 * This implementation returns the path hash code.
	 */
	@Override
	public int hashCode() {
		return path.hashCode();
	}

	@Override
	public String getPathWithinContext() {
		return path.toUri().getPath();
	}

	@Override
	public OutputStream getOutputStream() throws IOException {
		try {
			return fs.create(path, true);
		} finally {
			exists = true;
		}
	}

	@Override
	public boolean isWritable() {
		try {
			return ((exists && fs.isFile(path)) || (!exists));
		} catch (IOException ex) {
			return false;
		}
	}

	/**
	 * Returns the path.
	 *
	 * @return Returns the path
	 */
	Path getPath() {
		return path;
	}
}