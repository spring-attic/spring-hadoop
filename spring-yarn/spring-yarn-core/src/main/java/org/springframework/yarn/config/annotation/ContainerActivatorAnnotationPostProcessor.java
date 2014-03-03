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
package org.springframework.yarn.config.annotation;

import java.lang.reflect.Method;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.yarn.annotation.OnYarnContainerStart;
import org.springframework.yarn.container.ContainerHandler;

/**
 * Post-processor for Methods annotated with @{@link OnYarnContainerStart}.
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerActivatorAnnotationPostProcessor implements MethodAnnotationPostProcessor<OnYarnContainerStart>{

	protected final BeanFactory beanFactory;

	public ContainerActivatorAnnotationPostProcessor(ListableBeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

	@Override
	public Object postProcess(Object bean, String beanName, Method method, OnYarnContainerStart annotation) {
		return new ContainerHandler(bean, method);
	}

}
