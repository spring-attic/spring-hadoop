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
package org.springframework.yarn.config;

import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.event.DefaultYarnEventPublisher;
import org.springframework.yarn.support.YarnContextUtils;

/**
 * Bean factory post processor which adds creates default task
 * scheduler and executor if necessary.
 *
 * @author Janne Valkealahti
 *
 */
public class ConfiguringBeanFactoryPostProcessor implements BeanFactoryPostProcessor {

	private final static Log log = LogFactory.getLog(ConfiguringBeanFactoryPostProcessor.class);

	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
		if (beanFactory instanceof BeanDefinitionRegistry) {
			BeanDefinitionRegistry registry = (BeanDefinitionRegistry) beanFactory;
			if (!beanFactory.containsBean(YarnContextUtils.TASK_SCHEDULER_BEAN_NAME)) {
				registerTaskScheduler(registry);
			}
			if (!beanFactory.containsBean(YarnContextUtils.TASK_EXECUTOR_BEAN_NAME)) {
				registerTaskExecutor(registry);
			}
			if (!beanFactory.containsBean(YarnSystemConstants.DEFAULT_ID_EVENT_PUBLISHER)) {
				registerYarnEventPublisher(registry);
			}
		}
	}

	/**
	 * Register task scheduler.
	 *
	 * @param registry the registry
	 */
	private void registerTaskScheduler(BeanDefinitionRegistry registry) {
		if (log.isInfoEnabled()) {
			log.info("No bean named '" + YarnContextUtils.TASK_SCHEDULER_BEAN_NAME
					+ "' has been explicitly defined. Therefore, a default ThreadPoolTaskScheduler will be created.");
		}
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(ThreadPoolTaskScheduler.class);
		builder.addPropertyValue("poolSize", 10);
		builder.addPropertyValue("threadNamePrefix", "task-scheduler-");
		builder.addPropertyValue("rejectedExecutionHandler", new CallerRunsPolicy());
		BeanComponentDefinition schedulerComponent = new BeanComponentDefinition(builder.getBeanDefinition(),
				YarnContextUtils.TASK_SCHEDULER_BEAN_NAME);
		BeanDefinitionReaderUtils.registerBeanDefinition(schedulerComponent, registry);
	}

	/**
	 * Register task executor.
	 *
	 * @param registry the registry
	 */
	private void registerTaskExecutor(BeanDefinitionRegistry registry) {
		if (log.isInfoEnabled()) {
			log.info("No bean named '" + YarnContextUtils.TASK_EXECUTOR_BEAN_NAME
					+ "' has been explicitly defined. Therefore, a default SyncTaskExecutor will be created.");
		}
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(SyncTaskExecutor.class);
		BeanComponentDefinition schedulerComponent = new BeanComponentDefinition(builder.getBeanDefinition(),
				YarnContextUtils.TASK_EXECUTOR_BEAN_NAME);
		BeanDefinitionReaderUtils.registerBeanDefinition(schedulerComponent, registry);
	}

	/**
	 * Register yarn event publisher
	 *
	 * @param registry the registry
	 */
	private void registerYarnEventPublisher(BeanDefinitionRegistry registry) {
		if (log.isInfoEnabled()) {
			log.info("No bean named '" + YarnSystemConstants.DEFAULT_ID_EVENT_PUBLISHER
					+ "' has been explicitly defined. Therefore, a default YarnEventPublisher will be created.");
		}
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(DefaultYarnEventPublisher.class);
		BeanComponentDefinition eventComponent = new BeanComponentDefinition(builder.getBeanDefinition(),
				YarnSystemConstants.DEFAULT_ID_EVENT_PUBLISHER);
		BeanDefinitionReaderUtils.registerBeanDefinition(eventComponent, registry);
	}

}
