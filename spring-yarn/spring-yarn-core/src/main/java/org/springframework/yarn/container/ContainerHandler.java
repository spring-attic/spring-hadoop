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
package org.springframework.yarn.container;

import java.lang.reflect.Method;

import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.yarn.annotation.YarnContainer;

/**
 * Handler for a common object representing something to be run.
 * This is usually used when a plain pojo is configured with @{@link YarnContainer}
 * and @{@link OnYarnContainerStart} annotations.
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerHandler {

	private final Object target;
	private final Method method;

	/**
	 * Instantiates a new container handler.
	 *
	 * @param target the target bean
	 * @param method the target method
	 */
	public ContainerHandler(Object target, Method method) {
		Assert.notNull(target, "Target bean must be set");
		Assert.notNull(method, "Target bean method must be set");
		this.target = target;
		this.method = method;
	}

	/**
	 * Handle and run method associated with a bean target.
	 */
	public void handle() {
		ReflectionUtils.invokeMethod(method, target);
	}

}
