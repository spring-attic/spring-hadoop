/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.hadoop.cascading;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;

import cascading.cascade.Cascade;
import cascading.flow.Flow;
import cascading.management.UnitOfWork;
import cascading.stats.CascadingStats;

/**
 * Simple runner for executing {@link Cascade}s or {@link Flow}s. By default, the runner waits for the jobs to finish and returns its status.
 * 
 * @author Costin Leau
 */
public class CascadeRunner implements InitializingBean, DisposableBean, Callable<CascadingStats>, BeanFactoryAware {

	private static final Log log = LogFactory.getLog(CascadeRunner.class);

	private boolean waitToComplete = true;
	private boolean runAtStartup = true;
	private UnitOfWork<CascadingStats> uow;

	private List<String> preActions;
	private List<String> postActions;
	private BeanFactory beanFactory;

	@Override
	public void afterPropertiesSet() {
		if (runAtStartup) {
			call();
		}
	}

	@Override
	public void destroy() {
		if (uow != null) {
			uow.stop();
		}
	}

	@Override
	public CascadingStats call() {
		invoke(preActions);
		CascadingStats stats = Runner.run(uow, waitToComplete);
		invoke(postActions);

		return stats;
	}

	/**
	 * Sets the unit of work. Can be of type {@link Flow} or {@link Cascade}.
	 *
	 * @param uow the new unit of work.
	 */
	public void setUnitOfWork(UnitOfWork<CascadingStats> uow) {
		this.uow = uow;
	}

	/**
	 * Sets the run at startup.
	 *
	 * @param runAtStartup The runAtStartup to set.
	 */
	public void setRunAtStartup(boolean runAtStartup) {
		this.runAtStartup = runAtStartup;
	}

	/**
	 * Beans to be invoked before running the action.
	 * 
	 * @param beans
	 */
	public void setPreAction(String... beans) {
		this.preActions = CollectionUtils.arrayToList(beans);
	}

	/**
	 * Beans to be invoked after running the action.
	 * 
	 * @param beans
	 */
	public void setPostAction(String... beans) {
		this.postActions = CollectionUtils.arrayToList(beans);
	}

	private void invoke(List<String> beans) {
		if (beanFactory != null) {
			if (!CollectionUtils.isEmpty(beans)) {
				for (String bean : beans) {
					beanFactory.getBean(bean);
				}
			}
		}
		else {
			log.warn("No beanFactory set - cannot invoke pre/post actions [" + beans + "]");
		}
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}
}