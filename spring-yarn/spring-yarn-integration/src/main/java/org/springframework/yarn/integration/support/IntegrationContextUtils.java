/*
 * Copyright 2013-2014 the original author or authors.
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
package org.springframework.yarn.integration.support;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.Assert;

/**
 * Utility methods for accessing common integration components from the BeanFactory.
 *
 * @author Janne Valkealahti
 *
 */
public class IntegrationContextUtils {

	public static final String TASK_SCHEDULER_BEAN_NAME = "taskScheduler";

	// we match this name with the one from SI
	public static final String YARN_INTEGRATION_CONVERSION_SERVICE_BEAN_NAME = "integrationConversionService";

	/**
	 * Return the {@link TaskScheduler} bean whose name is "taskScheduler" if
	 * available.
	 *
	 * @param beanFactory BeanFactory for lookup, must not be null.
	 * @return task scheduler
	 */
	public static TaskScheduler getTaskScheduler(BeanFactory beanFactory) {
		return getBeanOfType(beanFactory, TASK_SCHEDULER_BEAN_NAME, TaskScheduler.class);
	}

	/**
	 * Return the {@link TaskScheduler} bean whose name is "taskScheduler".
	 *
	 * @param beanFactory BeanFactory for lookup, must not be null.
	 * @return task scheduler
	 * @throws IllegalStateException if no such bean is available
	 */
	public static TaskScheduler getRequiredTaskScheduler(BeanFactory beanFactory) {
		TaskScheduler taskScheduler = getTaskScheduler(beanFactory);
		Assert.state(taskScheduler != null, "No such bean '" + TASK_SCHEDULER_BEAN_NAME + "'");
		return taskScheduler;
	}

	/**
	 * Return the {@link ConversionService} bean whose name is
	 * "integrationConversionService" if available.
	 *
	 * @param beanFactory BeanFactory for lookup, must not be null.
	 * @return conversion service
	 */
	public static ConversionService getConversionService(BeanFactory beanFactory) {
		return getBeanOfType(beanFactory, YARN_INTEGRATION_CONVERSION_SERVICE_BEAN_NAME, ConversionService.class);
	}

	private static <T> T getBeanOfType(BeanFactory beanFactory, String beanName, Class<T> type) {
		Assert.notNull(beanFactory, "BeanFactory must not be null");
		if (!beanFactory.containsBean(beanName)) {
			return null;
		}
		return beanFactory.getBean(beanName, type);
	}

}
