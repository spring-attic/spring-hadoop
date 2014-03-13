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
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.io.DefaultResourceLoader;
//import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.Assert;
import org.springframework.util.PathMatcher;
import org.springframework.util.StringUtils;

/**
 * Spring ResourceLoader over Hadoop FileSystem.
 *
 * @author Costin Leau
 * @author Janne Valkealahti
 *
 */
public class HdfsResourceLoader extends DefaultResourceLoader implements ResourcePatternResolver,
		PriorityOrdered, Closeable, DisposableBean, InitializingBean {

	private static final Log log = LogFactory.getLog(HdfsResourceLoader.class);

	/** Pseudo URL prefix for loading from the hdfs path: "hdfs:" */
	private static final String HDFS_URL_PREFIX = "hdfs:";

	private final FileSystem fs;
	private final PathMatcher pathMatcher = new AntPathMatcher();

	/** Flag telling if fs is created in this class */
	private final boolean internalFS;

	private volatile boolean useCodecs = true;
	private volatile CompressionCodecFactory codecsFactory;

	/** Flag telling if path without prefix is routed to hdfs */
	private volatile boolean handleNoprefix = true;

	/** If we're impersonating a user */
	private String impersonatedUser = null;

	/** Needed to fall back to default spring functionality */
	private ResourcePatternResolver resourcePatternResolver;

	/**
	 * Constructs a new <code>HdfsResourceLoader</code> instance.
	 *
	 * @param config Hadoop configuration to use.
	 */
	public HdfsResourceLoader(Configuration config) {
		this(config, null);
	}

	/**
	 * Constructs a new <code>HdfsResourceLoader</code> instance.
	 *
	 * @param config Hadoop configuration to use.
	 * @param uri Hadoop file system URI.
	 * @param user Hadoop user for accessing the file system.
	 */
	@SuppressWarnings("resource")
	public HdfsResourceLoader(Configuration config, URI uri, String user) {
		Assert.notNull(config, "a valid configuration is required");

		impersonatedUser = user;
		internalFS = true;
		FileSystem tempFS = null;
		codecsFactory = new CompressionCodecFactory(config);

		try {
			if (uri == null) {
				uri = FileSystem.getDefaultUri(config);
			}
			tempFS = (StringUtils.hasText(impersonatedUser) ? FileSystem.get(uri, config, impersonatedUser) : FileSystem.get(uri, config));
		} catch (Exception ex) {
			tempFS = null;
			throw new IllegalStateException("Cannot create filesystem", ex);
		} finally {
			fs = tempFS;
		}
	}

	/**
	 * Constructs a new <code>HdfsResourceLoader</code> instance.
	 *
	 * @param config Hadoop configuration to use.
	 * @param uri Hadoop file system URI.
	 */
	public HdfsResourceLoader(Configuration config, URI uri) {
		this(config, uri, null);
	}

	/**
	 * Constructs a new <code>HdfsResourceLoader</code> instance.
	 *
	 * @param fs Hadoop file system to use.
	 */
	public HdfsResourceLoader(FileSystem fs) {
		Assert.notNull(fs, "a non-null file-system required");
		this.fs = fs;
		internalFS = false;
		codecsFactory = new CompressionCodecFactory(fs.getConf());
	}

	@Override
	protected Resource getResourceByPath(String path) {
		if (handleNoprefix) {
			return new HdfsResource(stripLeadingTilde(path), fs, codecs());
		} else {
			return super.getResourceByPath(path);
		}
	}

	@Override
	public Resource getResource(String location) {
		// it looks like spring DefaultResourceLoader will rely java.net.URL to throw
		// exception before if fall back to getResourceByPath. This is not reliable
		// so do explicit check if location starts with 'hdfs'.
		if (location.startsWith(HDFS_URL_PREFIX) || (location.indexOf(':') < 0 && handleNoprefix)) {
			return getResourceByPath(location);
		} else {
			return super.getResource(location);
		}
	}

	@Override
	public Resource[] getResources(String locationPattern) throws IOException {
		Assert.notNull(locationPattern, "Location pattern must not be null");

		if (locationPattern.startsWith(HDFS_URL_PREFIX) || (locationPattern.indexOf(':') < 0 && handleNoprefix)) {
			// Only look for a pattern after a prefix here
			// (to not get fooled by a pattern symbol in a strange prefix).
			if (pathMatcher.isPattern(stripPrefix(locationPattern))) {
				// a resource pattern
				return findPathMatchingResources(locationPattern);
			} else {
				// a single resource with the given name
				return new Resource[] { getResource(stripPrefix(stripLeadingTilde(locationPattern))) };
			}
		} else {
			return resourcePatternResolver.getResources(locationPattern);
		}
	}

	@Override
	public int getOrder() {
		return PriorityOrdered.HIGHEST_PRECEDENCE;
	}

	@Override
	public void destroy() throws IOException {
		close();
	}

	@Override
	public void close() throws IOException {
		if (fs != null && internalFS) {
			try {
				fs.close();
				// swallow bug in FS closing too early - HADOOP-4829
			} catch (NullPointerException npe) {
			}
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (resourcePatternResolver == null) {
			resourcePatternResolver = new PathMatchingResourcePatternResolver(this);
		}
	}

	@Override
	public ClassLoader getClassLoader() {
		return fs.getConf().getClassLoader();
	}

	/**
	 * Sets the handle noprefix.
	 *
	 * @param handleNoprefix the new handle noprefix
	 */
	public void setHandleNoprefix(boolean handleNoprefix) {
		this.handleNoprefix = handleNoprefix;
	}

	/**
	 * Returns the Hadoop file system used by this resource loader.
	 *
	 * @return the Hadoop file system in use.
	 */
	public FileSystem getFileSystem() {
		return fs;
	}

	/**
	 * Indicates whether to use (or not) the codecs found inside the Hadoop
	 * configuration. This affects the content of the streams backing this
	 * resource - whether the raw content is delivered as is
	 * or decompressed on the fly (if the configuration allows it so).
	 * The latter is the default.
	 *
	 * @param useCodecs whether to use any codecs defined in the Hadoop configuration
	 */
	public void setUseCodecs(boolean useCodecs) {
		this.useCodecs = useCodecs;
	}

	/**
	 * Sets the resource pattern resolver.
	 *
	 * @param resourcePatternResolver the new resource pattern resolver
	 */
	public void setResourcePatternResolver(ResourcePatternResolver resourcePatternResolver) {
		this.resourcePatternResolver = resourcePatternResolver;
	}

	/**
	 * Find all resources that match the given location pattern via the
	 * Ant-style PathMatcher.
	 *
	 * @param locationPattern the location pattern to match
	 * @return the result as Resource array
	 * @throws IOException in case of I/O errors
	 */
	protected Resource[] findPathMatchingResources(String locationPattern) throws IOException {
		String rootDirPath = determineRootDir(locationPattern);
		String subPattern = locationPattern.substring(rootDirPath.length());
		Resource[] rootDirResources = getResources(rootDirPath);
		Set<Resource> result = new LinkedHashSet<Resource>(16);
		for (Resource rootDirResource : rootDirResources) {
			result.addAll(doFindPathMatchingFileResources(rootDirResource, subPattern));
		}
		if (log.isDebugEnabled()) {
			log.debug("Resolved location pattern [" + locationPattern + "] to resources " + result);
		}
		return result.toArray(new Resource[result.size()]);
	}

	/**
	 * Find all resources in the hdfs file system that match the given location pattern
	 * via the Ant-style PathMatcher.
	 *
	 * @param rootDirResource the root directory as Resource
	 * @param subPattern the sub pattern to match (below the root directory)
	 * @return the Set of matching Resource instances
	 * @throws IOException in case of I/O errors
	 */
	protected Set<Resource> doFindPathMatchingFileResources(Resource rootDirResource, String subPattern)
			throws IOException {

		Path rootDir;
		try {
			rootDir = (rootDirResource instanceof HdfsResource ?
					((HdfsResource) rootDirResource).getPath() :
					new Path(rootDirResource.getURI().toString()));
		} catch (IOException ex) {
			if (log.isWarnEnabled()) {
				log.warn("Cannot search for matching files underneath " + rootDirResource +
						" because it does not correspond to a directory in the file system", ex);
			}
			return Collections.emptySet();
		}
		return doFindMatchingFileSystemResources(rootDir, subPattern);
	}

	/**
	 * Find all resources in the file system that match the given location pattern
	 * via the Ant-style PathMatcher.
	 *
	 * @param rootDir the root directory in the file system
	 * @param subPattern the sub pattern to match (below the root directory)
	 * @return the Set of matching Resource instances
	 * @throws IOException in case of I/O errors
	 * @see org.springframework.util.PathMatcher
	 */
	protected Set<Resource> doFindMatchingFileSystemResources(Path rootDir, String subPattern) throws IOException {
		if (log.isDebugEnabled()) {
			log.debug("Looking for matching resources in directory tree [" + rootDir.toUri().getPath() + "]");
		}
		Set<Path> matchingFiles = retrieveMatchingFiles(rootDir, subPattern);
		Set<Resource> result = new LinkedHashSet<Resource>(matchingFiles.size());
		for (Path path : matchingFiles) {
			result.add(new HdfsResource(path, fs, codecs()));
		}
		return result;
	}

	/**
	 * Retrieve files that match the given path pattern,
	 * checking the given directory and its subdirectories.
	 *
	 * @param rootDir the directory to start from
	 * @param pattern the pattern to match against,  * relative to the root directory
	 * @return the Set of matching Path instances
	 * @throws IOException if directory contents could not be retrieved
	 */
	@SuppressWarnings("deprecation")
	protected Set<Path> retrieveMatchingFiles(Path rootDir, String pattern) throws IOException {
		boolean exists = fs.exists(rootDir);
		if (!exists) {
			// Silently skip non-existing directories.
			if (log.isDebugEnabled()) {
				log.debug("Skipping [" + rootDir.toUri().getPath() + "] because it does not exist");
			}
			return Collections.emptySet();
		}
		// previous exists() should make sure we don't
		// get FileNotFoundException
		FileStatus fileStatus = fs.getFileStatus(rootDir);
		if (!fileStatus.isDir()) {
			// Complain louder if it exists but is no directory.
			if (log.isWarnEnabled()) {
				log.warn("Skipping [" + rootDir.toUri().getPath() + "] because it does not denote a directory");
			}
			return Collections.emptySet();
		}
		String fullPattern = StringUtils.replace(rootDir.toUri().getPath(), File.separator, "/");
		if (!pattern.startsWith("/")) {
			fullPattern += "/";
		}
		fullPattern = fullPattern + StringUtils.replace(pattern, File.separator, "/");
		Set<Path> result = new LinkedHashSet<Path>(8);
		doRetrieveMatchingFiles(fullPattern, rootDir, result);
		return result;
	}

	/**
	 * Recursively retrieve files that match the given pattern,
	 * adding them to the given result list.
	 *
	 * @param fullPattern the pattern to match against, with prepended root directory path
	 * @param dir the current directory
	 * @param result the Set of matching File instances to add to
	 * @throws IOException if directory contents could not be retrieved
	 */
	@SuppressWarnings("deprecation")
	protected void doRetrieveMatchingFiles(String fullPattern, Path dir, Set<Path> result) throws IOException {
		if (log.isDebugEnabled()) {
			log.debug("Searching directory [" + dir.toUri().getPath() +
					"] for files matching pattern [" + fullPattern + "]");
		}

		FileStatus[] dirContents = null;
		try {
			dirContents = fs.listStatus(dir);
		} catch (IOException ex) {
			// ignore (likely security exception)
		}

		if (dirContents == null) {
			if (log.isWarnEnabled()) {
				log.warn("Could not retrieve contents of directory [" + dir.toUri().getPath() + "]");
			}
			return;
		}
		for (FileStatus content : dirContents) {
			String currPath = StringUtils.replace(content.getPath().toUri().getPath(), File.separator, "/");
			if (content.isDir() && pathMatcher.matchStart(fullPattern, currPath + "/")) {
				doRetrieveMatchingFiles(fullPattern, content.getPath(), result);
			}
			if (pathMatcher.match(fullPattern, currPath)) {
				result.add(content.getPath());
			}
		}
	}

	/**
	 * Determine the root directory for the given location.
	 * <p>Used for determining the starting point for file matching,
	 * resolving the root directory location and passing it
	 * into {@code doFindPathMatchingPathResources}, with the
	 * remainder of the location as pattern.
	 * <p>Will return "/dir/" for the pattern "/dir/*.xml",
	 * for example.
	 *
	 * @param location the location to check
	 * @return the part of the location that denotes the root directory
	 */
	protected String determineRootDir(String location) {
		int rootDirEnd = location.length();
		while (rootDirEnd > 0 && pathMatcher.isPattern(location.substring(0,rootDirEnd))) {
			rootDirEnd = location.lastIndexOf('/', rootDirEnd - 2) + 1;
		}
		return location.substring(0, rootDirEnd);
	}

	/**
	 * Removes a leading tilde shortcut if exists.
	 */
	private String stripLeadingTilde(String locationPattern) {
		if (locationPattern.startsWith("~/")) {
			return locationPattern.substring(2);
		}
		return locationPattern;
	}

	private CompressionCodecFactory codecs() {
		return (useCodecs ? codecsFactory : null);
	}

	/**
	 * Removes a prefix from a given path and what's
	 * left is a real 'file' path
	 */
	private static String stripPrefix(String path) {
		String ret = null;
		try {
			ret = new Path(path).toUri().getPath();
		} catch (Exception e) {}
		if (ret == null && path.startsWith(HDFS_URL_PREFIX) && !path.startsWith("hdfs://")) {
			// check if path is 'hdfs:myfile.txt', strip prefix and colon
			ret = path.substring(5);
		}
		if (ret == null) {
			// fall back to given path
			ret = path;
		}
		return ret;
	}

}
