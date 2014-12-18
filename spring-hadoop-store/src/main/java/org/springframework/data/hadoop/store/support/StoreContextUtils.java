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
package org.springframework.data.hadoop.store.support;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.hadoop.store.StoreSystemConstants;
import org.springframework.data.hadoop.store.event.StoreEventPublisher;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.Assert;

/**
 * Utility methods for accessing common components from the BeanFactory.
 *
 * @author Janne Valkealahti
 *
 */
public class StoreContextUtils {

	/** Default task scheduler bean name */
	public static final String TASK_SCHEDULER_BEAN_NAME = "taskScheduler";

	/** Default task executor bean name */
	public static final String TASK_EXECUTOR_BEAN_NAME = "taskExecutor";

	/**
	 * Return the {@link TaskScheduler} bean whose name is "taskScheduler" if available.
	 *
	 * @param beanFactory BeanFactory for lookup, must not be null.
	 * @return the task scheduler
	 */
	public static TaskScheduler getTaskScheduler(BeanFactory beanFactory) {
		return getBeanOfType(beanFactory, TASK_SCHEDULER_BEAN_NAME, TaskScheduler.class);
	}

	/**
	 * Return the {@link TaskScheduler} bean whose name is "taskExecutor" if available.
	 *
	 * @param beanFactory BeanFactory for lookup, must not be null.
	 * @return the task executor
	 */
	public static TaskExecutor getTaskExecutor(BeanFactory beanFactory) {
		return getBeanOfType(beanFactory, TASK_EXECUTOR_BEAN_NAME, TaskExecutor.class);
	}

	/**
	 * Return the {@link StoreEventPublisher} bean whose name is "storeEventPublisher" if available.
	 *
	 * @param beanFactory BeanFactory for lookup, must not be null.
	 * @return the store event publisher
	 */
	public static StoreEventPublisher getEventPublisher(BeanFactory beanFactory) {
		return getBeanOfType(beanFactory, StoreSystemConstants.DEFAULT_ID_EVENT_PUBLISHER,
				StoreEventPublisher.class);
	}

	/**
	 * Gets a bean from a factory with a given name and type.
	 *
	 * @param beanFactory the bean factory
	 * @param beanName the bean name
	 * @param type the type as of a class
	 * @return Bean known to a bean factory, null if not found.
	 */
	private static <T> T getBeanOfType(BeanFactory beanFactory, String beanName, Class<T> type) {
		Assert.notNull(beanFactory, "BeanFactory must not be null");
		if (!beanFactory.containsBean(beanName)) {
			return null;
		}
		return beanFactory.getBean(beanName, type);
	}

}
