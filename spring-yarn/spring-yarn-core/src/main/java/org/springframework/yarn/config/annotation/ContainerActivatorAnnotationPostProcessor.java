/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.annotation.Order;
import org.springframework.core.annotation.OrderUtils;
import org.springframework.yarn.annotation.OnContainerStart;
import org.springframework.yarn.container.ContainerHandler;

/**
 * Post-processor for Methods annotated with @{@link OnContainerStart}.
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerActivatorAnnotationPostProcessor implements MethodAnnotationPostProcessor<OnContainerStart>{

	protected final BeanFactory beanFactory;

	public ContainerActivatorAnnotationPostProcessor(ListableBeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

	@Override
	public Object postProcess(Object bean, String beanName, Method method, OnContainerStart annotation) {
		ContainerHandler handler = new ContainerHandler(bean, method);
		Integer order = findOrder(bean, method);
		if (order != null) {
			handler.setOrder(order);
		}
		return handler;
	}

	/**
	 * Find {@link Order} order either from a class or
	 * method level. Method level always takes presence
	 * over class level.
	 *
	 * @param bean the bean to inspect
	 * @param method the method to inspect
	 * @return the order or NULL if not found
	 */
	private static Integer findOrder(Object bean, Method method) {
		Integer order = OrderUtils.getOrder(bean.getClass());
		Order ann = AnnotationUtils.findAnnotation(method, Order.class);
		if (ann != null) {
			order = ann.value();
		}
		return order;
	}

}
