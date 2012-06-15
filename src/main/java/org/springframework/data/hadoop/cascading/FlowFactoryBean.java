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

import java.util.Collection;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.flow.FlowSkipStrategy;
import cascading.flow.FlowStepStrategy;

/**
 * Base FactoryBean for creating Cascading {@link Flow}s.
 * 
 * @author Costin Leau
 */
abstract class FlowFactoryBean<T extends Flow<?>> implements InitializingBean, FactoryBean<T> {

	private FlowSkipStrategy skipStrategy;
	private FlowStepStrategy stepStrategy;
	private Collection<FlowListener> listeners;
	private Integer priority;

	private String writeDOT, writeStepsDOT;

	private T flow;

	@Override
	public T getObject() throws Exception {
		return flow;
	}

	@Override
	public Class<?> getObjectType() {
		return (flow != null ? flow.getClass() : Flow.class);
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Flow<?> flow = createFlow();

		if (skipStrategy != null) {
			flow.setFlowSkipStrategy(skipStrategy);
		}

		if (stepStrategy != null) {
			flow.setFlowStepStrategy(stepStrategy);
		}

		if (listeners != null) {
			for (FlowListener listener : listeners) {
				flow.addListener(listener);
			}
		}

		if (StringUtils.hasText(writeDOT)) {
			flow.writeDOT(writeDOT);
		}

		if (StringUtils.hasText(writeStepsDOT)) {
			flow.writeStepsDOT(writeStepsDOT);
		}

		if (priority != null) {
			flow.setSubmitPriority(priority);
		}
	}


	abstract T createFlow() throws Exception;

	/**
	 * Sets the skip strategy.
	 *
	 * @param skipStrategy The skipStrategy to set.
	 */
	public void setSkipStrategy(FlowSkipStrategy skipStrategy) {
		this.skipStrategy = skipStrategy;
	}

	/**
	 * Sets the step strategy.
	 *
	 * @param stepStrategy The stepStrategy to set.
	 */
	public void setStepStrategy(FlowStepStrategy stepStrategy) {
		this.stepStrategy = stepStrategy;
	}

	/**
	 * Sets the listeners.
	 *
	 * @param listeners The listeners to set.
	 */
	public void setListeners(Collection<FlowListener> listeners) {
		this.listeners = listeners;
	}

	/**
	 * Sets the write dot.
	 *
	 * @param writeDOT the new write dot
	 */
	public void setWriteDOT(String writeDOT) {
		this.writeDOT = writeDOT;
	}

	/**
	 * Sets the write steps dot.
	 *
	 * @param writeStepsDOT the new write steps dot
	 */
	public void setWriteStepsDOT(String writeStepsDOT) {
		this.writeStepsDOT = writeStepsDOT;
	}

	/**
	 * Sets the flow submit priority.
	 * 
	 * @param priority flow priority
	 */
	public void setPriority(Integer priority) {
		this.priority = priority;
	}
}