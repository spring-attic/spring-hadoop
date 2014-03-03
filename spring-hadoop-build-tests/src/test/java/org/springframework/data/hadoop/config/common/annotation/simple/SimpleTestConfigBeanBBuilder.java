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

import org.springframework.data.hadoop.config.common.annotation.AbstractConfiguredAnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.AnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.configurers.DefaultResourceConfigurer;
import org.springframework.data.hadoop.config.common.annotation.configurers.ResourceConfigurer;

/**
 * {@link AnnotationBuilder} for {@link SimpleTestConfigBeanB}.
 *
 * @author Janne Valkealahti
 *
 */
public class SimpleTestConfigBeanBBuilder
		extends AbstractConfiguredAnnotationBuilder<SimpleTestConfigBeanB, SimpleTestConfigBeanBConfigurer,SimpleTestConfigBeanBBuilder>
		implements SimpleTestConfigBeanBConfigurer {

	private String dataB;
	private String dataBB;

	@Override
	protected SimpleTestConfigBeanB performBuild() throws Exception {
		SimpleTestConfigBeanB bean = new SimpleTestConfigBeanB();
		bean.dataB = dataB;
		bean.dataBB = dataBB;
		return bean;
	}

	@Override
	public SimpleTestConfigBeanBConfigurer setData(String data) {
		this.dataB = data;
		return this;
	}

	@Override
	public SimpleTestConfigBeanBConfigurer setDataBB(String data) {
		this.dataBB = data;
		return this;
	}

	@Override
	public ResourceConfigurer<SimpleTestConfigBeanBConfigurer> withResources() throws Exception {
		return getOrApply(new DefaultResourceConfigurer<SimpleTestConfigBeanB,SimpleTestConfigBeanBConfigurer,SimpleTestConfigBeanBBuilder>());
	}

}
