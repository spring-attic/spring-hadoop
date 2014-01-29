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
package org.springframework.yarn.container;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Simple factory bean for {@link YarnContainer}s.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnContainerFactoryBean implements InitializingBean, FactoryBean<YarnContainer> {

	/** Yarn container class */
	private Class<? extends YarnContainer> containerClass;

	/** Yarn container reference */
	private YarnContainer containerRef;

	@Override
	public YarnContainer getObject() throws Exception {
		return containerRef;
	}

	@Override
	public Class<YarnContainer> getObjectType() {
		return YarnContainer.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		// 1) if we have a reference use that
		// 2) instantiate a class if given
		// 3) fall back to default container
		if(containerRef == null) {
			if (containerClass != null) {
				containerRef = BeanUtils.instantiate(containerClass);
			} else {
				containerRef = BeanUtils.instantiate(DefaultYarnContainer.class);
			}
		}
	}

	/**
	 * Sets the container class.
	 *
	 * @param containerClass the new container class
	 */
	public void setContainerClass(Class<? extends YarnContainer> containerClass) {
		this.containerClass = containerClass;
	}

	/**
	 * Sets the container ref.
	 *
	 * @param containerRef the new container ref
	 */
	public void setContainerRef(YarnContainer containerRef) {
		this.containerRef = containerRef;
	}

}
