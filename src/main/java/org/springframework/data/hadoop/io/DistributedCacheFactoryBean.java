/*
 * Copyright 2011-2012 the original author or authors.
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

import java.net.URI;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Factory for easy declarative configuration of a {@link DistributedCache}.
 * 
 * @author Costin Leau
 */
public class DistributedCacheFactoryBean implements InitializingBean, FactoryBean<DistributedCache> {

	public static class CacheEntry {
		public enum EntryType {
			LOCAL, CACHE, CP
		}

		final EntryType type;
		final String value;

		public CacheEntry(EntryType type, String value) {
			this.type = type;
			this.value = value;
		}
	}

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

		// fall back to system discovery
		if (fs == null) {
			fs = FileSystem.get(conf);
		}

		ds = new DistributedCache();

		if (createSymlink) {
			DistributedCache.createSymlink(conf);
		}

		HdfsResourceLoader loader = new HdfsResourceLoader(conf);

		for (CacheEntry entry : entries) {
			Resource[] resources = loader.getResources(entry.value);
			if (!ObjectUtils.isEmpty(resources)) {
				for (Resource resource : resources) {
					HdfsResource res = (HdfsResource) resource;

					URI uri = res.getURI();
					String defaultLink = resource.getFilename();
					boolean isArchive = (defaultLink.endsWith(".tgz") || defaultLink.endsWith(".tar")
							|| defaultLink.endsWith(".tar.gz") || defaultLink.endsWith(".zip"));

					switch (entry.type) {
					case CP:
						Path p = res.getPath();

						if (!StringUtils.hasText(p.toUri().getFragment())) {
							p = new Path(URI.create(p.toString() + "#" + defaultLink));
						}

						if (isArchive) {
							DistributedCache.addArchiveToClassPath(p, conf, fs);
						}
						else {
							DistributedCache.addFileToClassPath(p, conf, fs);
						}

						break;

					case LOCAL:

						//						if (!StringUtils.hasText(uri.getFragment())) {
						//							uri = URI.create(uri.toString() + "#" + defaultLink);
						//						}

						if (isArchive) {
							DistributedCache.addLocalArchives(conf, uri.toString());
						}
						else {
							DistributedCache.addLocalFiles(conf, uri.toString());
						}

						break;

					case CACHE:

						if (!StringUtils.hasText(uri.getFragment())) {
							uri = URI.create(uri.toString() + "#" + defaultLink);
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
	}

	/**
	 * @param entries The entries to set.
	 */
	public void setEntries(Collection<CacheEntry> entries) {
		this.entries = entries;
	}

	/**
	 * @param conf The conf to set.
	 */
	public void setConfiguration(Configuration conf) {
		this.conf = conf;
	}

	/**
	 * @param fs The fs to set.
	 */
	public void setFileSystem(FileSystem fs) {
		this.fs = fs;
	}

	/**
	 * @param createSymlink The createSymlink to set.
	 */
	public void setCreateSymlink(boolean createSymlink) {
		this.createSymlink = createSymlink;
	}
}