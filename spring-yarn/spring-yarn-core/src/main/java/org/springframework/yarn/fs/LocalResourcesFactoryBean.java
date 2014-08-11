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
package org.springframework.yarn.fs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

/**
 * Factory bean building {@link ResourceLocalizer}s objects.
 *
 * @author Janne Valkealahti
 *
 */
public class LocalResourcesFactoryBean implements InitializingBean, FactoryBean<ResourceLocalizer> {

	/** Localizer returned from this factory */
	private ResourceLocalizer resources;

	/** Localizer transfer entries */
	private Collection<TransferEntry> hdfsEntries;

	/** Localizer additional transfer entries */
	private final HashMap<String, Collection<TransferEntry>> additionalHdfsEntries = new HashMap<String, Collection<TransferEntry>>();

	/** Localizer copy entries*/
	private Collection<CopyEntry> copyEntries;

	private Collection<RawCopyEntry> rawEntries;

	/** Yarn configuration*/
	private Configuration configuration;

	/** Staging directory if set*/
	private Path stagingDirectory;

	// defaults
	private LocalResourceType defaultType;
	private LocalResourceVisibility defaultVisibility;

	@Override
	public ResourceLocalizer getObject() throws Exception {
		return resources;
	}

	@Override
	public Class<ResourceLocalizer> getObjectType() {
		return ResourceLocalizer.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void afterPropertiesSet() throws Exception {

		// defaults if defined
		for(TransferEntry entry : hdfsEntries) {
			if(entry.type == null) {
				entry.type = (defaultType != null ? defaultType : LocalResourceType.FILE);
			}
			if(entry.visibility == null) {
				entry.visibility = (defaultVisibility != null ? defaultVisibility : LocalResourceVisibility.APPLICATION);
			}
		}

		DefaultResourceLocalizer defaultResourceLocalizer = new DefaultResourceLocalizer(configuration, hdfsEntries, copyEntries);
		if (stagingDirectory != null) {
			defaultResourceLocalizer.setStagingDirectory(stagingDirectory);
		}

		if (rawEntries != null) {
			Map<String, byte[]> rawFileContents = new HashMap<String, byte[]>();
			for (RawCopyEntry e : rawEntries) {
				rawFileContents.put(e.dest, e.src);
			}
			defaultResourceLocalizer.setRawFileContents(rawFileContents);
		}

		if (additionalHdfsEntries.size() == 0) {
			resources = defaultResourceLocalizer;
		} else {
			Map<String, ResourceLocalizer> localizers = new HashMap<String, ResourceLocalizer>();
			for (Entry<String, Collection<TransferEntry>> entry : additionalHdfsEntries.entrySet()) {
				for(TransferEntry e : entry.getValue()) {
					if(e.type == null) {
						e.type = (defaultType != null ? defaultType : LocalResourceType.FILE);
					}
					if(e.visibility == null) {
						e.visibility = (defaultVisibility != null ? defaultVisibility : LocalResourceVisibility.APPLICATION);
					}
				}
				localizers.put(entry.getKey(), new DefaultResourceLocalizer(configuration, entry.getValue(),
						new ArrayList<LocalResourcesFactoryBean.CopyEntry>()));
			}
			resources = new DefaultMultiResourceLocalizer(defaultResourceLocalizer, localizers);
		}
	}

	/**
	 * Sets default {@link LocalResourceType} for entry.
	 *
	 * @param defaultType the default {@link LocalResourceType}
	 */
	public void setType(LocalResourceType defaultType) {
		this.defaultType = defaultType;
	}

	/**
	 * Sets default {@link LocalResourceVisibility} for entry.
	 *
	 * @param defaultVisibility the default {@link LocalResourceVisibility}
	 */
	public void setVisibility(LocalResourceVisibility defaultVisibility) {
		this.defaultVisibility = defaultVisibility;
	}

	/**
	 * Sets hdfs entries reference for this factory.
	 *
	 * @param hdfsEntries Collection of hdfs entries
	 */
	public void setHdfsEntries(Collection<TransferEntry> hdfsEntries) {
		this.hdfsEntries = hdfsEntries;
	}

	/**
	 * Sets hdfs entries reference for this factory.
	 *
	 * @param id the identifier
	 * @param hdfsEntries Collection of hdfs entries
	 */
	public void setHdfsEntries(String id, Collection<TransferEntry> hdfsEntries) {
		this.additionalHdfsEntries.put(id, hdfsEntries);
	}

	/**
	 * Sets copy entries reference for this factory.
	 *
	 * @param copyEntries Collection of copy entries
	 */
	public void setCopyEntries(Collection<CopyEntry> copyEntries) {
		this.copyEntries = copyEntries;
	}

	/**
	 * Sets Yarn configuration for this factory.
	 *
	 * @param configuration Yarn configuration
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Sets the staging directory.
	 *
	 * @param stagingDirectory the new staging directory
	 * @see #setStagingDirectory(Path)
	 */
	public void setStagingDirectory(String stagingDirectory) {
		if (StringUtils.hasText(stagingDirectory)) {
			setStagingDirectory(new Path(stagingDirectory));
		}
	}

	/**
	 * Sets the staging directory.
	 *
	 * @param stagingDirectory the new staging directory
	 */
	public void setStagingDirectory(Path stagingDirectory) {
		this.stagingDirectory = stagingDirectory;
	}

	public void setRawCopyEntries(Collection<RawCopyEntry> rawEntries) {
		this.rawEntries = rawEntries;
	}

	/**
	 * Helper class storing transfer entries.
	 */
	public static class TransferEntry {

		LocalResourceType type;
		LocalResourceVisibility visibility;
		String path;
		boolean staging;

		public TransferEntry(LocalResourceType type, LocalResourceVisibility visibility,
				String path, boolean staging) {
			super();
			this.type = type;
			this.visibility = visibility;
			this.path = path;
			this.staging = staging;
		}

	}

	/**
	 * Helper class storing copy entries.
	 */
	public static class CopyEntry {

		String src;
		String dest;
		boolean staging;

		public CopyEntry(String src, String dest, boolean staging) {
			this.src = src;
			this.dest = dest;
			this.staging = staging;
		}

	}

	/**
	 * Helper class storing raw copy entries.
	 */
	public static class RawCopyEntry {

		byte[] src;
		String dest;
		boolean staging;

		public RawCopyEntry(byte[] src, String dest, boolean staging) {
			this.src = src;
			this.dest = dest;
			this.staging = staging;
		}

	}

}
