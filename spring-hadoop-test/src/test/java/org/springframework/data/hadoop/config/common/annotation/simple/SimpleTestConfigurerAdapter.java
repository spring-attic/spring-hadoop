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
package org.springframework.data.hadoop.config.common.annotation.simple;

import org.springframework.data.hadoop.config.common.annotation.AnnotationBuilder;

/**
 * Generic adapter example which user would extend
 * in @{@link org.springframework.context.annotation.Configuration}
 *
 * @author Janne Valkealahti
 *
 */
public class SimpleTestConfigurerAdapter implements SimpleTestConfigurer {

	private SimpleTestConfigBeanABuilder beanABuilder;
	private SimpleTestConfigBeanBBuilder beanBBuilder;

	@Override
	public final void init(SimpleTestConfigBuilder config) throws Exception {
		config.setSharedObject(String.class, "simpleSharedData");
		config.setSharedObject(SimpleTestConfigBeanABuilder.class, getSimpleTestConfigBeanABuilder());
		config.setSharedObject(SimpleTestConfigBeanBBuilder.class, getSimpleTestConfigBeanBBuilder());
	}

	@Override
	public void configure(SimpleTestConfigBuilder config) throws Exception {
	}

	@Override
	public void configure(SimpleTestConfigBeanABuilder a) throws Exception {
	}

	@Override
	public void configure(SimpleTestConfigBeanBConfigurer b) throws Exception {
	}

	protected final SimpleTestConfigBeanBBuilder getSimpleTestConfigBeanBBuilder() throws Exception {
		if (beanBBuilder != null) {
			return beanBBuilder;
		}
		beanBBuilder = new SimpleTestConfigBeanBBuilder();
		configure(beanBBuilder);
		return beanBBuilder;
	}

	protected final SimpleTestConfigBeanABuilder getSimpleTestConfigBeanABuilder() throws Exception {
		if (beanABuilder != null) {
			return beanABuilder;
		}
		beanABuilder = new SimpleTestConfigBeanABuilder();
		configure(beanABuilder);
		return beanABuilder;
	}

	@Override
	public boolean isAssignable(AnnotationBuilder<SimpleTestConfig> builder) {
		return builder instanceof SimpleTestConfigBuilder;
	}

}
