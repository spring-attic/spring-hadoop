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

import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.hadoop.store.event.StoreEventPublisher;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.Assert;

/**
 * Convenient base class for object which needs spring task scheduler, task executor and life cycle handling.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class LifecycleObjectSupport implements InitializingBean, SmartLifecycle, BeanFactoryAware {

	private static final Log log = LogFactory.getLog(LifecycleObjectSupport.class);

	// fields for lifecycle
	private volatile boolean autoStartup = true;

	private volatile int phase = 0;

	private volatile boolean running;

	// lock to protect lifycycle methods
	private final ReentrantLock lifecycleLock = new ReentrantLock();

	// common task handling
	private TaskScheduler taskScheduler;

	private TaskExecutor taskExecutor;

	// to access bean factory
	private volatile BeanFactory beanFactory;

	/** Context application event publisher if exist */
	private volatile StoreEventPublisher storeEventPublisher;

	@Override
	public final void afterPropertiesSet() {
		try {
			this.onInit();
		}
		catch (Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			}
			throw new BeanInitializationException("failed to initialize", e);
		}
	}

	@Override
	public final void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		Assert.notNull(beanFactory, "beanFactory must not be null");
		if (log.isDebugEnabled()) {
			log.debug("Setting bean factory: " + beanFactory + " for " + this);
		}
		this.beanFactory = beanFactory;
	}

	@Override
	public final boolean isAutoStartup() {
		return this.autoStartup;
	}

	@Override
	public final int getPhase() {
		return this.phase;
	}

	@Override
	public final boolean isRunning() {
		this.lifecycleLock.lock();
		try {
			return this.running;
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Override
	public final void start() {
		this.lifecycleLock.lock();
		try {
			if (!this.running) {
				this.doStart();
				this.running = true;
				if (log.isInfoEnabled()) {
					log.info("started " + this);
				}
				else {
					if (log.isDebugEnabled()) {
						log.debug("already started " + this);
					}
				}
			}
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Override
	public final void stop() {
		this.lifecycleLock.lock();
		try {
			if (this.running) {
				this.doStop();
				this.running = false;
				if (log.isInfoEnabled()) {
					log.info("stopped " + this);
				}
			}
			else {
				if (log.isDebugEnabled()) {
					log.debug("already stopped " + this);
				}
			}
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Override
	public final void stop(Runnable callback) {
		this.lifecycleLock.lock();
		try {
			this.stop();
			callback.run();
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	/**
	 * Sets the auto startup.
	 *
	 * @param autoStartup the new auto startup
	 * @see SmartLifecycle
	 */
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * Sets the phase.
	 *
	 * @param phase the new phase
	 * @see SmartLifecycle
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}

	/**
	 * Gets the {@link BeanFactory} for this instance.
	 *
	 * @return the bean factory.
	 */
	protected final BeanFactory getBeanFactory() {
		return beanFactory;
	}

	/**
	 * Sets the used {@link TaskScheduler}.
	 *
	 * @param taskScheduler the task scheduler
	 */
	public void setTaskScheduler(TaskScheduler taskScheduler) {
		Assert.notNull(taskScheduler, "taskScheduler must not be null");
		this.taskScheduler = taskScheduler;
	}

	/**
	 * Gets the defined {@link TaskScheduler}.
	 *
	 * @return the defined task scheduler
	 */
	protected TaskScheduler getTaskScheduler() {
		if (taskScheduler == null && getBeanFactory() != null) {
			if (log.isDebugEnabled()) {
				log.debug("getting taskScheduler service from bean factory " + getBeanFactory());
			}
			taskScheduler = StoreContextUtils.getTaskScheduler(getBeanFactory());
		}
		return taskScheduler;
	}

	/**
	 * Sets the used {@link TaskExecutor}.
	 *
	 * @param taskExecutor the task executor
	 */
	public void setTaskExecutor(TaskExecutor taskExecutor) {
		Assert.notNull(taskExecutor, "taskExecutor must not be null");
		this.taskExecutor = taskExecutor;
	}

	/**
	 * Gets the store event publisher.
	 *
	 * @return the store event publisher
	 */
	public StoreEventPublisher getStoreEventPublisher() {
		if (storeEventPublisher == null && getBeanFactory() != null) {
			if (log.isDebugEnabled()) {
				log.debug("getting yarnEventPublisher service from bean factory " + getBeanFactory());
			}
			storeEventPublisher = StoreContextUtils.getEventPublisher(getBeanFactory());
		}
		return storeEventPublisher;
	}

	/**
	 * Sets the store event publisher.
	 *
	 * @param storeEventPublisher the new store event publisher
	 */
	public void setStoreEventPublisher(StoreEventPublisher storeEventPublisher) {
		Assert.notNull(storeEventPublisher, "StoreEventPublisher cannot be null");
		this.storeEventPublisher = storeEventPublisher;
	}

	/**
	 * Gets the defined {@link TaskExecutor}.
	 *
	 * @return the defined task executor
	 */
	protected TaskExecutor getTaskExecutor() {
		if (taskExecutor == null && getBeanFactory() != null) {
			if (log.isDebugEnabled()) {
				log.debug("getting taskExecutor service from bean factory " + getBeanFactory());
			}
			taskExecutor = StoreContextUtils.getTaskExecutor(getBeanFactory());
		}
		return taskExecutor;
	}

	/**
	 * Subclasses may implement this for initialization logic. Called during the {@link InitializingBean} phase.
	 * Implementor should always call super method not to break initialization chain.
	 */
	protected void onInit() throws Exception {
	}

	/**
	 * Subclasses may implement this method with the start behaviour. This method will be invoked while holding the
	 * {@link #lifecycleLock}.
	 */
	protected void doStart() {
	};

	/**
	 * Subclasses may implement this method with the stop behaviour. This method will be invoked while holding the
	 * {@link #lifecycleLock}.
	 */
	protected void doStop() {
	};

}
