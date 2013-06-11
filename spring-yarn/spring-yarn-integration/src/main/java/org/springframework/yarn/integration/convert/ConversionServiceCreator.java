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
package org.springframework.yarn.integration.convert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.support.ConversionServiceFactoryBean;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.yarn.integration.support.IntegrationContextUtils;

/**
 * Post processor which automatically creates an instance
 * of conversion service if it doesn't exists.
 *
 * @author Oleg Zhurakousky
 * @author Mark Fisher
 * @author Janne Valkealahti
 *
 */
class ConversionServiceCreator implements BeanFactoryPostProcessor {

	private final Log logger = LogFactory.getLog(this.getClass());

	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
		if (!beanFactory.containsBean(IntegrationContextUtils.YARN_INTEGRATION_CONVERSION_SERVICE_BEAN_NAME)) {
			if (beanFactory instanceof BeanDefinitionRegistry) {
				BeanDefinitionBuilder conversionServiceBuilder =
						BeanDefinitionBuilder.rootBeanDefinition(CustomConversionServiceFactoryBean.class);
				BeanDefinitionHolder beanDefinitionHolder = new BeanDefinitionHolder(
						conversionServiceBuilder.getBeanDefinition(),
						IntegrationContextUtils.YARN_INTEGRATION_CONVERSION_SERVICE_BEAN_NAME);
				BeanDefinitionReaderUtils.registerBeanDefinition(beanDefinitionHolder, (BeanDefinitionRegistry) beanFactory);
			}
			else if (logger.isWarnEnabled()) {
				logger.warn("BeanFactory is not a BeanDefinitionRegistry implementation. Cannot register a default ConversionService.");
			}
		}
	}


	/**
	 * This is a workaround until we depend on Spring 3.1 and specifically when SPR-8818 is resolved.
	 */
	static class CustomConversionServiceFactoryBean extends ConversionServiceFactoryBean {

		@Override
		public ConversionService getObject() {
			ConversionService service = super.getObject();
			if (service instanceof GenericConversionService) {
				((GenericConversionService) service).removeConvertible(Object.class, Object.class);
			}
			return service;
		}
	}

}
