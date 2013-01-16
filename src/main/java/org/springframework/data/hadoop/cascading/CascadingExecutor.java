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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;

import cascading.cascade.Cascade;
import cascading.flow.Flow;
import cascading.management.UnitOfWork;
import cascading.stats.CascadingStats;

/**
 * Common class used for executing Cascading uow.
 * 
 * @author Costin Leau
 */
class CascadingExecutor implements DisposableBean {

	protected boolean waitToComplete = true;
	protected UnitOfWork<? extends CascadingStats> uow;

	protected Log log = LogFactory.getLog(getClass());

	protected CascadingStats execute() {
		if (waitToComplete) {
			uow.complete();

		}
		else {
			uow.start();
		}

		return uow.getStats();
	}

	@Override
	public void destroy() {
		if (uow != null) {
			uow.stop();
		}
	}

	/**
	 * Sets the unit of work. Can be of type {@link Flow} or {@link Cascade}.
	 *
	 * @param uow the new unit of work.
	 */
	public void setUnitOfWork(UnitOfWork<? extends CascadingStats> uow) {
		this.uow = uow;
	}

	/**
	 * Indicates whether the 'runner' should wait for the UnitOfWork to complete (default).
	 *
	 * @return whether the runner waits for the unit to complete or not.
	 */
	public boolean isWaitForCompletion() {
		return waitToComplete;
	}

	/**
	 * Indicates whether the 'runner' should wait for the UnitOfWork to complete (default)
	 * after submission or not.
	 * 
	 * @param waitForCompletion whether to wait for the unit to complete or not.
	 */
	public void setWaitForCompletion(boolean waitForCompletion) {
		this.waitToComplete = waitForCompletion;
	}
}