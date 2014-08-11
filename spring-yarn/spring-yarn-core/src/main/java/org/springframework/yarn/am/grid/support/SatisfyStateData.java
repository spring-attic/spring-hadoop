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
package org.springframework.yarn.am.grid.support;

import java.util.ArrayList;
import java.util.List;

import org.springframework.yarn.am.allocate.ContainerAllocateData;
import org.springframework.yarn.am.grid.GridMember;

public class SatisfyStateData {

	private List<GridMember> remove;

	private ContainerAllocateData allocateData;

	public SatisfyStateData() {
		this(new ArrayList<GridMember>(), new ContainerAllocateData());
	}

	public SatisfyStateData(List<GridMember> remove) {
		this(remove, new ContainerAllocateData());
	}

	public SatisfyStateData(ContainerAllocateData allocateData) {
		this(new ArrayList<GridMember>(), allocateData);
	}

	public SatisfyStateData(List<GridMember> remove, ContainerAllocateData allocateData) {
		this.remove = remove;
		this.allocateData = allocateData;
	}

	public List<GridMember> getRemoveData() {
		return remove;
	}

	public void setRemoveData(List<GridMember> remove) {
		this.remove = remove;
	}

	public ContainerAllocateData getAllocateData() {
		return allocateData;
	}

	public void setAllocateData(ContainerAllocateData allocateData) {
		this.allocateData = allocateData;
	}

}
