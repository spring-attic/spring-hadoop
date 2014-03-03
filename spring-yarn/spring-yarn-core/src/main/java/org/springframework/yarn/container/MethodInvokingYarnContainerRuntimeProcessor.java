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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * A simple {@link YarnContainerRuntimeProcessor} implementation using
 * methods from a container.
 *
 * @author Janne Valkealahti
 *
 * @param <T> the return type
 */
public class MethodInvokingYarnContainerRuntimeProcessor<T> implements YarnContainerRuntimeProcessor<T> {

	private final ContainerMethodInvokerHelper<T> delegate;

	public MethodInvokingYarnContainerRuntimeProcessor(Object targetObject, Method method) {
		delegate = new ContainerMethodInvokerHelper<T>(targetObject, method);
	}

	public MethodInvokingYarnContainerRuntimeProcessor(Object targetObject, String methodName) {
		delegate = new ContainerMethodInvokerHelper<T>(targetObject, methodName);
	}

	public MethodInvokingYarnContainerRuntimeProcessor(Object targetObject, Class<? extends Annotation> annotationType) {
		delegate = new ContainerMethodInvokerHelper<T>(targetObject, annotationType);
	}


	@Override
	public T process(YarnContainerRuntime yarnContainerRuntime) {
		try {
			return delegate.process(yarnContainerRuntime);
		} catch (Exception e) {
			throw new YarnRuntimeException("Error processing container", e);
		}
	}

}
