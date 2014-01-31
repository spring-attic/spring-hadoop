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
package org.springframework.yarn.config.annotation;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.yarn.config.AbstractYarnNamespaceHandler;
import org.springframework.yarn.config.ConfiguringBeanFactoryPostProcessor;

/**
 * {@link Configuration} which registers
 * {@link org.springframework.yarn.config.ConfiguringBeanFactoryPostProcessor}
 * as {@link BeanFactoryPostProcessor} order to create some default beans
 * similarly to xml config.
 *
 * @author Janne Valkealahti
 * @see EnableYarn
 */
@Configuration
public class ConfiguringBeanFactoryPostProcessorConfiguration {

	@Bean(name=AbstractYarnNamespaceHandler.CONFIGURING_POSTPROCESSOR_BEAN_NAME)
	public static BeanFactoryPostProcessor getBeanFactoryPostProcessor() {
		return new ConfiguringBeanFactoryPostProcessor();
	}

}
