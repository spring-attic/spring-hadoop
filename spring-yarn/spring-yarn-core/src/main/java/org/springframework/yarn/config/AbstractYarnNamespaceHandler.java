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

import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.xml.NamespaceHandlerSupport;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

/**
 * Base namespace handler which registers a bean factory post processor order to
 * get default beans created automatically.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractYarnNamespaceHandler extends NamespaceHandlerSupport {

	public static final String CONFIGURING_POSTPROCESSOR_BEAN_NAME =
			"org.springframework.yarn.config.internal.ConfiguringBeanFactoryPostProcessor";

	@Override
	public BeanDefinition parse(Element element, ParserContext parserContext) {
		registerConfiguringBeanFactoryPostProcessorIfNecessary(parserContext);
		return super.parse(element, parserContext);
	}

	/**
	 * Register configuring bean factory post processor if necessary.
	 *
	 * @param parserContext the parser context
	 */
	private void registerConfiguringBeanFactoryPostProcessorIfNecessary(ParserContext parserContext) {
		boolean alreadyRegistered = false;
		if (parserContext.getRegistry() instanceof ListableBeanFactory) {
			alreadyRegistered = ((ListableBeanFactory) parserContext.getRegistry())
					.containsBean(CONFIGURING_POSTPROCESSOR_BEAN_NAME);
		} else {
			alreadyRegistered = parserContext.getRegistry().isBeanNameInUse(CONFIGURING_POSTPROCESSOR_BEAN_NAME);
		}
		if (!alreadyRegistered) {
			BeanDefinitionBuilder postProcessorBuilder = BeanDefinitionBuilder
					.genericBeanDefinition(ConfiguringBeanFactoryPostProcessor.class);
			BeanDefinitionHolder postProcessorHolder = new BeanDefinitionHolder(
					postProcessorBuilder.getBeanDefinition(), CONFIGURING_POSTPROCESSOR_BEAN_NAME);
			BeanDefinitionReaderUtils.registerBeanDefinition(postProcessorHolder, parserContext.getRegistry());
		}
	}

}
