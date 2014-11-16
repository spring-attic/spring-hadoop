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

import org.springframework.core.Ordered;
import org.springframework.yarn.annotation.OnContainerStart;
import org.springframework.yarn.annotation.YarnComponent;

/**
 * Handler for a common object representing something to be run.
 * This is usually used when a plain pojo is configured with @{@link YarnComponent}
 * and @{@link OnContainerStart} annotations.
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerHandler implements Ordered {

	private final YarnContainerRuntimeProcessor<?> processor;

	private int order = Ordered.LOWEST_PRECEDENCE;

	/**
	 * Instantiates a new container handler.
	 *
	 * @param target the target bean
	 */
	public ContainerHandler(Object target) {
		this(new MethodInvokingYarnContainerRuntimeProcessor<Object>(target, OnContainerStart.class));
	}

	/**
	 * Instantiates a new container handler.
	 *
	 * @param target the target bean
	 * @param method the method
	 */
	public ContainerHandler(Object target, Method method) {
		this(new MethodInvokingYarnContainerRuntimeProcessor<Object>(target, method));
	}

	/**
	 * Instantiates a new container handler.
	 *
	 * @param target the target bean
	 * @param methodName the method name
	 */
	public ContainerHandler(Object target, String methodName) {
		this(new MethodInvokingYarnContainerRuntimeProcessor<Object>(target, methodName));
	}

	/**
	 * Instantiates a new container handler.
	 *
	 * @param <T> the generic type
	 * @param processor the processor
	 */
	public <T> ContainerHandler(MethodInvokingYarnContainerRuntimeProcessor<T> processor) {
		this.processor = processor;
	}

	@Override
	public int getOrder() {
		return order;
	}

	/**
	 * Sets the order used get value from {@link #getOrder()}.
	 * Default value is {@link Ordered#LOWEST_PRECEDENCE}.
	 *
	 * @param order the new order
	 */
	public void setOrder(int order) {
		this.order = order;
	}

	/**
	 * Handle container using a {@link YarnContainerRuntimeProcessor}.
	 *
	 * @param yarnContainerRuntime the yarn container runtime
	 * @return the result value
	 */
	public Object handle(YarnContainerRuntime yarnContainerRuntime) {
		return processor.process(yarnContainerRuntime);
	}

}
