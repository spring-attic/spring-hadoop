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
package org.springframework.data.hadoop.io;

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
import org.springframework.core.io.ContextResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Resource abstraction over HDFS {@link Path}s.
 *  
 * @author Costin Leau
 */
class HdfsResource implements WritableResource, ContextResource {

	private final String location;
	private final Path path;
	private final FileSystem fs;
	private boolean exists;
	private final FileStatus status;

	HdfsResource(String location, FileSystem fs) {
		this(location, null, fs);
	}

	HdfsResource(String parent, String child, FileSystem fs) {
		this(StringUtils.hasText(child) ? new Path(parent, child) : new Path(parent), fs);
	}

	HdfsResource(Path path, FileSystem fs) {
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
	}


	public long contentLength() throws IOException {
		if (exists) {
			if (status != null) {
				return status.getLen();
			}
		}
		throw new IOException("Cannot access the status for " + getDescription());
	}

	public Resource createRelative(String relativePath) throws IOException {
		return new HdfsResource(location, relativePath, fs);
	}

	public boolean exists() {
		return exists;
	}

	public String getDescription() {
		return "HDFS Resource for [" + location + "]";
	}

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

	public String getFilename() {
		return path.getName();
	}

	public URI getURI() throws IOException {
		return path.toUri();
	}

	public URL getURL() throws IOException {
		return path.toUri().toURL();
	}

	public boolean isOpen() {
		return (exists ? true : false);
	}

	public boolean isReadable() {
		return (exists ? true : false);
	}

	public long lastModified() throws IOException {
		if (exists && status != null) {
			return status.getModificationTime();
		}
		throw new IOException("Cannot get timestamp for " + getDescription());
	}

	public InputStream getInputStream() throws IOException {
		if (exists) {
			return fs.open(path);
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

	public String getPathWithinContext() {
		return path.getName();
	}

	public OutputStream getOutputStream() throws IOException {
		try {
			return fs.create(path, true);
		} finally {
			exists = true;
		}
	}

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