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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.yarn.listener.ContainerStateListener.ContainerState;

/**
 * Default implementation of a {@link YarnContainer}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultYarnContainer extends AbstractYarnContainer implements BeanFactoryAware {

	private final static Log log = LogFactory.getLog(DefaultYarnContainer.class);

	private BeanFactory beanFactory;

	@Override
	protected void runInternal() {
		Exception runtimeException = null;
		Object result = null;

		try {
			ContainerHandler containerHandler = beanFactory.getBean(ContainerHandler.class);
			result = containerHandler.handle(this);
		} catch (Exception e) {
			runtimeException = e;
			log.error("Error handling container", e);
		}

		log.info("Container state based on result=[" + result + "] runtimeException=[" + runtimeException + "]");

		if (runtimeException != null) {
			notifyContainerState(ContainerState.FAILED, 1);
		} else if (result != null && result instanceof Integer) {
			int val = ((Integer)result).intValue();
			if (val < 0) {
				notifyContainerState(ContainerState.FAILED, val);
			} else {
				notifyContainerState(ContainerState.COMPLETED, val);
			}
		} else if (result != null && result instanceof Boolean) {
			boolean val = ((Boolean)result).booleanValue();
			if (val) {
				notifyContainerState(ContainerState.COMPLETED, 0);
			} else {
				notifyContainerState(ContainerState.FAILED, -1);
			}
		} else {
			notifyCompleted();
		}
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

}
