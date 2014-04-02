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
package org.springframework.yarn.config.annotation.configurers;

import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurerAdapter;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.allocate.DefaultContainerAllocator;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterBuilder;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterConfigurer;
import org.springframework.yarn.support.ParsingUtils;

public class DefaultMasterContainerAllocatorConfigurer
		extends AnnotationConfigurerAdapter<YarnAppmaster, YarnAppmasterConfigurer, YarnAppmasterBuilder>
		implements MasterContainerAllocatorConfigurer {

	private Integer priority;
	private String memory;
	private Integer virtualCores;
	private boolean locality;

	@Override
	public void configure(YarnAppmasterBuilder builder) throws Exception {
		DefaultContainerAllocator containerAllocator = new DefaultContainerAllocator();
		if (priority != null) {
			containerAllocator.setPriority(priority);
		}
		if (virtualCores != null) {
			containerAllocator.setVirtualcores(virtualCores);
		}
		if (memory != null) {
			containerAllocator.setMemory(ParsingUtils.parseBytesAsMegs(memory));
		}
		containerAllocator.setLocality(locality);
		builder.setContainerAllocator(containerAllocator);
	}

	@Override
	public MasterContainerAllocatorConfigurer priority(Integer priority) {
		this.priority = priority;
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

	public MasterContainerAllocatorConfigurer locality(boolean locality) {
		this.locality = locality;
		return this;
	}

}
