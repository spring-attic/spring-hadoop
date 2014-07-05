/*
 * Copyright 2011-2014 the original author or authors.
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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.DistributedCacheFactoryBean.CacheEntry.EntryType;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Factory for easy declarative configuration of a {@link DistributedCache}.
 *
 * @author Costin Leau
 * @author Thomas Risberg
 */
@SuppressWarnings("deprecation")
public class DistributedCacheFactoryBean implements InitializingBean, FactoryBean<DistributedCache> {

	/**
	 * Class describing an entry of the distributed cache.
	 *
	 * @author Costin Leau
	 */
	public static class CacheEntry {
		/**
		 * A distributed cache entry type.
		 *
		 * @author Costin Leau
		 */
		public enum EntryType {
			/** Local entry */
			LOCAL,
			/** Cache wide entry*/
			CACHE,
			/** Classpath entry*/
			CP
		}

		final EntryType type;
		final String value;

		/**
		 * Constructs a new <code>CacheEntry</code> instance.
		 *
		 * @param type entry type
		 * @param value entry value
		 */
		public CacheEntry(EntryType type, String value) {
			this.type = type;
			this.value = value;
		}
	}

	private static boolean FILE_SEPARATOR_WARNING = true;

	private Configuration conf;
	private DistributedCache ds;
	private FileSystem fs;
	private boolean createSymlink = false;
	private Collection<CacheEntry> entries;

	@Override
	public DistributedCache getObject() throws Exception {
		return ds;
	}

	@Override
	public Class<?> getObjectType() {
		return DistributedCache.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(conf, "A Hadoop configuration is required");
		Assert.notEmpty(entries, "No entries specified");

		// fall back to system discovery
		if (fs == null) {
			fs = FileSystem.get(conf);
		}

		ds = new DistributedCache();

		if (createSymlink) {
			DistributedCache.createSymlink(conf);
		}

		HdfsResourceLoader loader = new HdfsResourceLoader(fs);

		boolean warnCpEntry = !":".equals(System.getProperty("path.separator"));

		try {
			for (CacheEntry entry : entries) {
				Resource[] resources = loader.getResources(entry.value);
				if (!ObjectUtils.isEmpty(resources)) {
					for (Resource resource : resources) {
						HdfsResource res = (HdfsResource) resource;

						URI uri = res.getURI();
						String path = getPathWithFragment(uri);

						String defaultLink = resource.getFilename();
						boolean isArchive = (defaultLink.endsWith(".tgz") || defaultLink.endsWith(".tar")
								|| defaultLink.endsWith(".tar.gz") || defaultLink.endsWith(".zip"));

						switch (entry.type) {
						case CP:
							// Path does not handle fragments so use the URI instead
							Path p = new Path(URI.create(path));

							if (FILE_SEPARATOR_WARNING && warnCpEntry) {
								LogFactory.getLog(DistributedCacheFactoryBean.class).warn(
										"System path separator is not ':' - this will likely cause invalid classpath entries within the DistributedCache. See the docs and HADOOP-9123 for more information.");
								// show the warning once per CL
								FILE_SEPARATOR_WARNING = false;
							}

							if (isArchive) {
								DistributedCache.addArchiveToClassPath(p, conf, fs);
							}
							else {
								DistributedCache.addFileToClassPath(p, conf, fs);
							}

							break;

						case LOCAL:

							// TODO - Need to figure out how to add local files
							break;

						case CACHE:

							if (!path.contains("#")) {
								// use the path to avoid adding the host:port into the uri
								uri = URI.create(path + "#" + defaultLink);
							}

							if (isArchive) {
								DistributedCache.addCacheArchive(uri, conf);
							}
							else {
								DistributedCache.addCacheFile(uri, conf);
							}

							break;
						}
					}
				}
			}
		} finally {
			loader.close();
		}
	}

	/**
	 * Sets the entries to be added to the distributed cache.
	 *
	 * @param entries The entries to set.
	 */
	public void setEntries(Collection<CacheEntry> entries) {
		this.entries = entries;
	}

	/**
	 * Sets the local entries to be added to the distributed cache.
	 *
	 * @param resources The entries to set.
	 */

	public void setLocalEntries(Collection<Resource> resources) {
		setEntries(EntryType.LOCAL, resources);
	}

	/**
	 * Sets the cache entries to be added to the distributed cache.
	 *
	 * @param resources The entries to set.
	 */
	public void setCacheEntries(Collection<Resource> resources) {
		setEntries(EntryType.CACHE, resources);
	}

	/**
	 * Sets the class-path entries to be added to the distributed cache.
	 *
	 * @param resources The entries to set.
	 */
	public void setClassPathEntries(Collection<Resource> resources) {
		setEntries(EntryType.CP, resources);
	}

	private void setEntries(EntryType cp, Collection<Resource> resources) {
		if (resources == null) {
			setEntries(null);
		}

		else {
			Collection<CacheEntry> entries = new ArrayList<CacheEntry>(resources.size());
			for (Resource resource : resources) {
				try {
					entries.add(new CacheEntry(cp, resource.getURI().toString()));
				} catch (IOException ex) {
					throw new IllegalArgumentException("Cannot resolve resource " + resource, ex);
				}
			}
			setEntries(entries);
		}
	}

	/**
	 * Sets the Hadoop configuration for the cache.
	 *
	 * @param conf The conf to set.
	 */
	public void setConfiguration(Configuration conf) {
		this.conf = conf;
	}

	/**
	 * Sets the Hadoop file system for this cache.
	 *
	 * @param fs File system to set.
	 */
	public void setFileSystem(FileSystem fs) {
		this.fs = fs;
	}

	/**
	 * Indicates whether to create symlinks or not.
	 *
	 * @param createSymlink whether to create symlinks or not.
	 */
	public void setCreateSymlink(boolean createSymlink) {
		this.createSymlink = createSymlink;
	}

	private static String getPathWithFragment(URI uri) {
		String path = uri.getPath();
		String fragment = uri.getFragment();
		if (StringUtils.hasText(fragment)) {
			path = path + "#" + fragment;
		}
		return path;
	}
}