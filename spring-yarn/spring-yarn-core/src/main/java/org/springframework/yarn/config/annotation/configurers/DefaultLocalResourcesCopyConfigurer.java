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
package org.springframework.yarn.config.annotation.configurers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurer;
import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurerAdapter;
import org.springframework.util.ObjectUtils;
import org.springframework.yarn.config.annotation.builders.YarnResourceLocalizerBuilder;
import org.springframework.yarn.config.annotation.builders.YarnResourceLocalizerConfigurer;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.RawCopyEntry;
import org.springframework.yarn.fs.ResourceLocalizer;

/**
 * {@link AnnotationConfigurer} which knows how to handle
 * copy entries in {@link ResourceLocalizer}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultLocalResourcesCopyConfigurer
		extends AnnotationConfigurerAdapter<ResourceLocalizer, YarnResourceLocalizerConfigurer, YarnResourceLocalizerBuilder>
		implements LocalResourcesCopyConfigurer {

	private Collection<CopyEntry> copyEntries = new ArrayList<CopyEntry>();

	private Collection<RawCopyEntry> rawEntries = new ArrayList<RawCopyEntry>();

	@Override
	public void configure(YarnResourceLocalizerBuilder builder) throws Exception {
		builder.setCopyEntries(copyEntries);
		if (rawEntries.size() > 0) {
			builder.setRawCopyEntries(rawEntries);
		}
	}

	@Override
	public LocalResourcesCopyConfigurer copy(String src, String dest, boolean staging) {
		copyEntries.add(new CopyEntry(src, dest, staging));
		return this;
	}

	@Override
	public LocalResourcesCopyConfigurer copy(String[] srcs, String dest, boolean staging) {
		if (!ObjectUtils.isEmpty(srcs)) {
			for (String src : srcs) {
				copyEntries.add(new CopyEntry(src, dest, staging));
			}
		}

		return this;
	}

	@Override
	public LocalResourcesCopyConfigurer raw(String fileName, byte[] content, String dest) {
		rawEntries.add(new RawCopyEntry(content, dest + fileName, false));
		return this;
	}

	@Override
	public LocalResourcesCopyConfigurer raw(Map<String, byte[]> raw, String dest) {
		if (raw != null) {
			for (Entry<String, byte[]> entry : raw.entrySet()) {
				raw(entry.getKey(), entry.getValue(), dest);
			}
		}
		return this;
	}

	@Override
	public ConfiguredCopyEntry source(String source) {
		return new ConfiguredCopyEntry(source);
	}

	public final class ConfiguredCopyEntry {
		private String source;
		private String destination;
		private ConfiguredCopyEntry(String source) {
			this.source = source;
		}
		public ConfiguredCopyEntry destination(String destination) {
			this.destination = destination;
			return this;
		}
		public LocalResourcesCopyConfigurer staging(boolean staging) {
			copyEntries.add(new CopyEntry(source, destination, staging));
			return DefaultLocalResourcesCopyConfigurer.this;
		}
	}

}
