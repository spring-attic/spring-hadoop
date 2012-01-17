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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.PathMatcher;

/**
 * Spring ResourceLoader over Hadoop FileSystem.
 * 
 * @author Costin Leau
 */
public class HdfsResourceLoader implements ResourcePatternResolver, PriorityOrdered, Closeable, DisposableBean {

	private static final String PREFIX_DELIMITER = ":";

	private final FileSystem fs;
	private final PathMatcher pathMatcher = new AntPathMatcher();
	private final boolean internalFS;

	public HdfsResourceLoader(Configuration config) {
		this(config, null);
	}

	public HdfsResourceLoader(Configuration config, URI uri, String user) {
		internalFS = true;
		FileSystem tempFS = null;

		try {
			if (uri == null) {
				uri = FileSystem.getDefaultUri(config);
			}
			tempFS = (user != null ? FileSystem.get(uri, config, user) : FileSystem.get(uri, config));
		} catch (Exception ex) {
			tempFS = null;
			throw new IllegalStateException("Cannot create filesystem", ex);
		} finally {
			fs = tempFS;
		}
	}

	public HdfsResourceLoader(Configuration config, URI uri) {
		this(config, uri, null);
	}

	public HdfsResourceLoader(FileSystem fs) {
		Assert.notNull(fs, "a non-null file-system required");
		this.fs = fs;
		internalFS = false;
	}

	public ClassLoader getClassLoader() {
		return fs.getConf().getClassLoader();
	}

	public Resource getResource(String location) {
		return new HdfsResource(location, fs);
	}

	public Resource[] getResources(String locationPattern) throws IOException {
		// Only look for a pattern after a prefix here
		// (to not get fooled by a pattern symbol in a strange prefix).
		if (pathMatcher.isPattern(stripPrefix(locationPattern))) {
			// a resource pattern
			return findPathMatchingResources(locationPattern);
		}
		else {
			// a single resource with the given name
			return new Resource[] { getResource(locationPattern) };
		}
	}

	protected Resource[] findPathMatchingResources(String locationPattern) throws IOException {
		// replace ~/ shortcut
		if (locationPattern.startsWith("~/")) {
			locationPattern = locationPattern.substring(2);
		}

		String rootDirPath = determineRootDir(locationPattern);
		String subPattern = locationPattern.substring(rootDirPath.length());
		if (rootDirPath.isEmpty()) {
			rootDirPath = ".";
		}
		Resource rootDirResource = getResource(rootDirPath);

		Set<Resource> result = new LinkedHashSet<Resource>(16);
		result.addAll(doFindPathMatchingPathResources(rootDirResource, subPattern));

		return result.toArray(new Resource[result.size()]);
	}

	protected String determineRootDir(String location) {
		int prefixEnd = location.indexOf(PREFIX_DELIMITER) + 1;
		int rootDirEnd = location.length();

		while (rootDirEnd > prefixEnd && pathMatcher.isPattern(location.substring(prefixEnd, rootDirEnd))) {
			rootDirEnd = location.lastIndexOf('/', rootDirEnd - 2) + 1;
		}
		if (rootDirEnd == 0) {
			rootDirEnd = prefixEnd;
		}
		return location.substring(0, rootDirEnd);
	}

	private Set<Resource> doFindPathMatchingPathResources(Resource rootDirResource, String subPattern)
			throws IOException {

		Path rootDir;

		rootDir = (rootDirResource instanceof HdfsResource ? ((HdfsResource) rootDirResource).getPath() : new Path(
				rootDirResource.getURI().toString()));

		Set<Resource> results = new LinkedHashSet<Resource>();
		String pattern = subPattern;

		if (!pattern.startsWith("/")) {
			pattern = "/".concat(pattern);
		}

		doRetrieveMatchingResources(rootDir, pattern, results);

		return results;
	}

	private void doRetrieveMatchingResources(Path rootDir, String subPattern, Set<Resource> results) throws IOException {
		if (!fs.isFile(rootDir)) {
			FileStatus[] statuses = null;
			try {
				statuses = fs.listStatus(rootDir);
			} catch (IOException ex) {
				// ignore (likely security exception)
			}

			if (!ObjectUtils.isEmpty(statuses)) {
				String root = rootDir.toUri().getPath();
				for (FileStatus fileStatus : statuses) {
					Path p = fileStatus.getPath();
					String location = p.toUri().getPath();
					if (location.startsWith(root)) {
						location = location.substring(root.length());
					}
					if (fileStatus.isDir() && pathMatcher.matchStart(subPattern, location)) {
						doRetrieveMatchingResources(p, subPattern, results);
					}

					else if (pathMatcher.match(subPattern, location)) {
						results.add(new HdfsResource(p, fs));
					}
				}
			}
		}

		// Remove "if" to allow folders to be added as well
		else if (pathMatcher.match(subPattern, stripPrefix(rootDir.toUri().getPath()))) {
			results.add(new HdfsResource(rootDir, fs));
		}
	}

	private static String stripPrefix(String path) {
		// strip prefix
		int index = path.indexOf(PREFIX_DELIMITER);
		return (index > -1 ? path.substring(index + 1) : path);
	}

	public int getOrder() {
		return PriorityOrdered.HIGHEST_PRECEDENCE;
	}

	public void destroy() throws IOException {
		close();
	}

	@Override
	public void close() throws IOException {
		if (fs != null & internalFS) {
			fs.close();
		}
	}
}