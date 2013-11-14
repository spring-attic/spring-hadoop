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

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.configuration.ConfigurationFactoryBean;
import org.w3c.dom.Element;

/**
 * Simple namespace parser for yarn:configuration.
 *
 * @author Janne Valkealahti
 *
 */
class YarnConfigParser extends AbstractPropertiesConfiguredBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return ConfigurationFactoryBean.class;
	}

	@Override
	protected boolean isEligibleAttribute(String attributeName) {
		return (!"resources".equals(attributeName)) && super.isEligibleAttribute(attributeName);
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		super.doParse(element, parserContext, builder);
		NamespaceUtils.setCSVProperty(element, builder, "resources");
	}

	@Override
	protected String defaultId(ParserContext context, Element element) {
		return YarnSystemConstants.DEFAULT_ID_CONFIGURATION;
	}

}
