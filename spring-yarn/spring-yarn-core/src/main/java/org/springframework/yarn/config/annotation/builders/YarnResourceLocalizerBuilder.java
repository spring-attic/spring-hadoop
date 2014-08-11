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
package org.springframework.yarn.config.annotation.builders;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.springframework.data.hadoop.config.common.annotation.AbstractConfiguredAnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.AnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.ObjectPostProcessor;
import org.springframework.yarn.config.annotation.configurers.DefaultLocalResourcesCopyConfigurer;
import org.springframework.yarn.config.annotation.configurers.DefaultLocalResourcesHdfsConfigurer;
import org.springframework.yarn.config.annotation.configurers.LocalResourcesCopyConfigurer;
import org.springframework.yarn.config.annotation.configurers.LocalResourcesHdfsConfigurer;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.RawCopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;
import org.springframework.yarn.fs.ResourceLocalizer;

/**
 * {@link AnnotationBuilder} for {@link ResourceLocalizer}.
 *
 * @author Janne Valkealahti
 *
 */
public final class YarnResourceLocalizerBuilder
		extends AbstractConfiguredAnnotationBuilder<ResourceLocalizer, YarnResourceLocalizerConfigurer, YarnResourceLocalizerBuilder>
		implements YarnResourceLocalizerConfigurer {

	private Configuration configuration;
	private String stagingDirectory;
	private LocalResourceType defaultType = LocalResourceType.FILE;
	private LocalResourceVisibility defaultVisibility = LocalResourceVisibility.APPLICATION;
	private Collection<CopyEntry> copyEntries;
	private Collection<TransferEntry> transferEntries;
	private Collection<RawCopyEntry> rawEntries;

	private final HashMap<String, Collection<TransferEntry>> transferEntries2 = new HashMap<String, Collection<TransferEntry>>();

	/**
	 * Instantiates a new yarn resource localizer builder.
	 */
	public YarnResourceLocalizerBuilder() {
		super(ObjectPostProcessor.QUIESCENT_POSTPROCESSOR, true);
	}

	@Override
	protected ResourceLocalizer performBuild() throws Exception {
		LocalResourcesFactoryBean fb = new LocalResourcesFactoryBean();
		fb.setType(defaultType);
		fb.setStagingDirectory(stagingDirectory);
		fb.setVisibility(defaultVisibility);
		fb.setConfiguration(configuration);
		fb.setCopyEntries(copyEntries != null ? copyEntries : new ArrayList<LocalResourcesFactoryBean.CopyEntry>());
		fb.setHdfsEntries(transferEntries != null ? transferEntries : new ArrayList<LocalResourcesFactoryBean.TransferEntry>());
		for (Entry<String, Collection<TransferEntry>> entry : transferEntries2.entrySet()) {
			fb.setHdfsEntries(entry.getKey(), entry.getValue());
		}
		if (rawEntries != null) {
			fb.setRawCopyEntries(rawEntries);
		}
		fb.afterPropertiesSet();
		return fb.getObject();
	}

	@Override
	public LocalResourcesCopyConfigurer withCopy() throws Exception {
		return apply(new DefaultLocalResourcesCopyConfigurer());
	}

	@Override
	public LocalResourcesHdfsConfigurer withHdfs() throws Exception {
		return apply(new DefaultLocalResourcesHdfsConfigurer());
	}

	@Override
	public LocalResourcesHdfsConfigurer withHdfs(String id) throws Exception {
		return apply(new DefaultLocalResourcesHdfsConfigurer(id));
	}

	@Override
	public YarnResourceLocalizerConfigurer stagingDirectory(String stagingDirectory) {
		this.stagingDirectory = stagingDirectory;
		return this;
	}

	public void configuration(Configuration configuration) {
		this.configuration = configuration;
	}

	public YarnResourceLocalizerConfigurer defaultLocalResourceType(LocalResourceType type) {
		defaultType = type;
		return this;
	}

	public YarnResourceLocalizerConfigurer defaultLocalResourceVisibility(LocalResourceVisibility visibility) {
		defaultVisibility = visibility;
		return this;
	}

	public void setCopyEntries(Collection<CopyEntry> copyEntries) {
		this.copyEntries = copyEntries;
	}

	public void setHdfsEntries(Collection<TransferEntry> transferEntries) {
		this.transferEntries = transferEntries;
	}

	public void setHdfsEntries(String id, Collection<TransferEntry> transferEntries) {
		this.transferEntries2.put(id, transferEntries);
	}

	public void setRawCopyEntries(Collection<RawCopyEntry> rawEntries) {
		this.rawEntries = rawEntries;
	}

}
