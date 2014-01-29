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
package org.springframework.yarn.fs;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * Factory bean building {@link ResourceLocalizer}s objects.
 *
 * @author Janne Valkealahti
 *
 */
public class LocalResourcesFactoryBean implements InitializingBean, FactoryBean<ResourceLocalizer> {

	/** Localizer returned from this factory */
	private ResourceLocalizer resources;

	/** Localizer transfer entries*/
	private Collection<TransferEntry> hdfsEntries;

	/** Localizer copy entries*/
	private Collection<CopyEntry> copyEntries;

	private Collection<RawCopyEntry> rawEntries;

	/** Yarn configuration*/
	private Configuration configuration;

	// defaults
	private LocalResourceType defaultType;
	private LocalResourceVisibility defaultVisibility;
	private String defaultLocal;
	private String defaultRemote;

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

		String defaultFs = configuration.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);

		// defaults if defined
		for(TransferEntry entry : hdfsEntries) {
			if(entry.type == null) {
				entry.type = (defaultType != null ? defaultType : LocalResourceType.FILE);
			}
			if(entry.visibility == null) {
				entry.visibility = (defaultVisibility != null ? defaultVisibility : LocalResourceVisibility.APPLICATION);
			}
			if(entry.local == null) {
				entry.local = (defaultLocal != null ? defaultLocal : defaultFs);
			}
			if(entry.remote == null) {
				entry.remote = (defaultRemote != null ? defaultRemote : defaultFs);
			}
			Assert.isTrue(entry.local != null && entry.remote != null, "Entry local/remote hdfs address can't be null");
		}
		DefaultResourceLocalizer defaultResourceLocalizer = new DefaultResourceLocalizer(configuration, hdfsEntries, copyEntries);
		if (rawEntries != null) {
			Map<String, byte[]> rawFileContents = new HashMap<String, byte[]>();
			for (RawCopyEntry e : rawEntries) {
				rawFileContents.put(e.dest, e.src);
			}
			defaultResourceLocalizer.setRawFileContents(rawFileContents);
		}

		resources = defaultResourceLocalizer;
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
	 * Sets default local hdfs base address for entry.
	 *
	 * @param defaultLocal hdfs base address
	 */
	public void setLocal(String defaultLocal) {
		this.defaultLocal = defaultLocal;
	}

	/**
	 * Sets default remote hdfs base address for entry.
	 *
	 * @param defaultRemote hdfs base address
	 */
	public void setRemote(String defaultRemote) {
		this.defaultRemote = defaultRemote;
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
		String local;
		String remote;
		boolean staging;

		public TransferEntry(LocalResourceType type, LocalResourceVisibility visibility,
				String path, String local, String remote, boolean staging) {
			super();
			this.type = type;
			this.visibility = visibility;
			this.path = path;
			this.local = local;
			this.remote = remote;
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
