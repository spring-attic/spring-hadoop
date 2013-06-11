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
package org.springframework.yarn.am;

import org.springframework.util.Assert;
import org.springframework.yarn.am.allocate.ContainerAllocator;
import org.springframework.yarn.am.container.ContainerLauncher;
import org.springframework.yarn.am.monitor.ContainerMonitor;

/**
 * Extension of {@link AbstractAppmaster} which adds
 * a common container services needed for usual
 * application master.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractServicesAppmaster extends AbstractAppmaster {

	/** Container allocator for this class */
	private ContainerAllocator allocator;

	/** Container launcher for this class */
	private ContainerLauncher launcher;

	/** Container monitor for this class */
	private ContainerMonitor monitor;

	/**
	 * Gets a used {@link ContainerAllocator} for this class.
	 *
	 * @return {@link ContainerAllocator} used in this class
	 */
	public ContainerAllocator getAllocator() {
		return allocator;
	}

	/**
	 * Sets the {@link ContainerAllocator} used for this class.
	 * This should be called before {@link #onInit() onInit} for this class
	 * is called.
	 *
	 * @param allocator the {@link ContainerAllocator}
	 */
	public void setAllocator(ContainerAllocator allocator) {
		Assert.isNull(this.allocator, "ContainerAllocator is already set");
		this.allocator = allocator;
	}

	/**
	 * Gets a used {@link ContainerLauncher} for this class.
	 *
	 * @return {@link ContainerLauncher} used in this class
	 */
	public ContainerLauncher getLauncher() {
		return launcher;
	}

	/**
	 * Sets the {@link ContainerLauncher} used for this class.
	 * This should be called before {@link #onInit() onInit} for this class
	 * is called.
	 *
	 * @param launcher the {@link ContainerLauncher}
	 */
	public void setLauncher(ContainerLauncher launcher) {
		Assert.isNull(this.launcher, "ContainerLauncher is already set");
		this.launcher = launcher;
	}

	/**
	 * Gets a used {@link ContainerMonitor} for this class.
	 *
	 * @return {@link ContainerMonitor} used in this class
	 */
	public ContainerMonitor getMonitor() {
		return monitor;
	}

	/**
	 * Sets the {@link ContainerMonitor} used for this class.
	 * This should be called before {@link #onInit() onInit} for this class
	 * is called.
	 *
	 * @param monitor the {@link ContainerMonitor}
	 */
	public void setMonitor(ContainerMonitor monitor) {
		Assert.isNull(this.monitor, "ContainerMonitor is already set");
		this.monitor = monitor;
	}

}
