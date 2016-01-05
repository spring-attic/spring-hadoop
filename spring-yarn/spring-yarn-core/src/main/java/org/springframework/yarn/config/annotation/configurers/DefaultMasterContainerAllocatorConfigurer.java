/*
 * Copyright 2014-2016 the original author or authors.
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
import java.util.List;

import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurer;
import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurerAdapter;
import org.springframework.util.StringUtils;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.allocate.DefaultContainerAllocator;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterBuilder;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterConfigurer;
import org.springframework.yarn.support.ParsingUtils;

/**
 * {@link AnnotationConfigurer} for {@link YarnAppmaster} container allocator.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultMasterContainerAllocatorConfigurer
		extends AnnotationConfigurerAdapter<YarnAppmaster, YarnAppmasterConfigurer, YarnAppmasterBuilder>
		implements MasterContainerAllocatorConfigurer {

	private Integer priority;
	private String labelExpression;
	private String memory;
	private Integer virtualCores;
	private boolean locality;

	private final List<DefaultMasterContainerAllocatorCollectionConfigurer> collectionConfigurers =
			new ArrayList<DefaultMasterContainerAllocatorCollectionConfigurer>();

	@Override
	public void configure(YarnAppmasterBuilder builder) throws Exception {
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		if (priority != null) {
			allocator.setPriority(priority);
		}
		if (labelExpression != null) {
			allocator.setLabelExpression(labelExpression);
		}
		if (virtualCores != null) {
			allocator.setVirtualcores(virtualCores);
		}
		if (memory != null) {
			allocator.setMemory(ParsingUtils.parseBytesAsMegs(memory));
		}
		allocator.setLocality(locality);

		for (DefaultMasterContainerAllocatorCollectionConfigurer configurer : collectionConfigurers) {
			allocator.setAllocationValues(configurer.id, configurer.priority, configurer.labelExpression, configurer.virtualCores,
					StringUtils.hasText(configurer.memory) ? ParsingUtils.parseBytesAsMegs(configurer.memory) : null,
					configurer.locality);
		}

		builder.setContainerAllocator(allocator);
	}

	@Override
	public MasterContainerAllocatorConfigurer priority(Integer priority) {
		this.priority = priority;
		return this;
	}

	@Override
	public MasterContainerAllocatorConfigurer labelExpression(String labelExpression) {
		this.labelExpression = labelExpression;
		return this;
	}

	@Override
	public MasterContainerAllocatorConfigurer virtualCores(Integer virtualCores) {
		this.virtualCores = virtualCores;
		return this;
	}

	@Override
	public MasterContainerAllocatorConfigurer memory(String memory) {
		this.memory = memory;
		return this;
	}

	@Override
	public MasterContainerAllocatorConfigurer memory(int memory) {
		this.memory = Integer.toString(memory);
		return this;
	}

	@Override
	public MasterContainerAllocatorConfigurer locality(boolean locality) {
		this.locality = locality;
		return this;
	}

	@Override
	public MasterContainerAllocatorCollectionConfigurer withCollection(String id) {
		DefaultMasterContainerAllocatorCollectionConfigurer configurer =
				new DefaultMasterContainerAllocatorCollectionConfigurer(id);
		collectionConfigurers.add(configurer);
		return configurer;
	}

	public final class DefaultMasterContainerAllocatorCollectionConfigurer implements MasterContainerAllocatorCollectionConfigurer {

		private final String id;
		private Integer priority;
		private String labelExpression;
		private String memory;
		private Integer virtualCores;
		private boolean locality;

		public DefaultMasterContainerAllocatorCollectionConfigurer(String id) {
			this.id = id;
		}

		@Override
		public MasterContainerAllocatorCollectionConfigurer priority(Integer priority) {
			this.priority = priority;
			return this;
		}

		@Override
		public MasterContainerAllocatorCollectionConfigurer labelExpression(String labelExpression) {
			this.labelExpression = labelExpression;
			return this;
		}

		@Override
		public MasterContainerAllocatorCollectionConfigurer virtualCores(Integer virtualCores) {
			this.virtualCores = virtualCores;
			return this;
		}

		@Override
		public MasterContainerAllocatorCollectionConfigurer memory(String memory) {
			this.memory = memory;
			return this;
		}

		@Override
		public MasterContainerAllocatorCollectionConfigurer memory(int memory) {
			this.memory = Integer.toString(memory);
			return this;
		}

		@Override
		public MasterContainerAllocatorCollectionConfigurer locality(boolean locality) {
			this.locality = locality;
			return this;
		}

		@Override
		public MasterContainerAllocatorConfigurer and() {
			return DefaultMasterContainerAllocatorConfigurer.this;
		}

	}

}
