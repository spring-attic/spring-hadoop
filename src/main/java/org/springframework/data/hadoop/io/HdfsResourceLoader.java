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

import java.io.IOException;
import java.net.URL;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.PathMatcher;

/**
 * Spring ResourceLoader over Hadoop FileSystem.
 * 
 * @author Costin Leau
 */
public class HdfsResourceLoader implements ResourcePatternResolver, PriorityOrdered {

	private static final Log log = LogFactory.getLog(HdfsResourceLoader.class);

	private static final String PREFIX_DELIMITER = ":";

	private final FileSystem fs;
	private final PathMatcher pathMatcher = new AntPathMatcher();

	public HdfsResourceLoader(FileSystem fs) {
		this.fs = fs;
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
		String rootDirPath = determineRootDir(locationPattern);
		String subPattern = locationPattern.substring(rootDirPath.length());
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
				rootDirResource.getURI()));

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
			FileStatus[] statuses = fs.listStatus(rootDir);

			for (FileStatus fileStatus : statuses) {
				Path p = fileStatus.getPath();
				String location = stripPrefix(p.toUri().getPath());
				if (fileStatus.isDir() && pathMatcher.matchStart(subPattern, location)) {
					doRetrieveMatchingResources(p, subPattern, results);
				}

				else if (pathMatcher.match(subPattern, location)) {
					results.add(new HdfsResource(p, fs));
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

	/**
	 * @param registerJvmUrl The registerJvmUrl to set.
	 */
	public void setRegisterJvmUrl(boolean registerJvmUrl) {
		if (registerJvmUrl) {
			try {
				URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(fs.getConf()));
				log.info("Registered HDFS URL stream handler");
			} catch (Throwable ex) {
				log.warn("Cannot register Hadoop URL stream handler - one is already registered?", ex);
			}
		}
	}

	public int getOrder() {
		return PriorityOrdered.HIGHEST_PRECEDENCE;
	}
}