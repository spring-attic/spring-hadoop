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

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.container.YarnContainerFactoryBean;
import org.w3c.dom.Element;

/**
 * Simple namespace parser for yarn:container.
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerParser extends AbstractSingleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return YarnContainerFactoryBean.class;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		YarnNamespaceUtils.setValueIfAttributeDefined(builder, element, "container-class");
		YarnNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "container-ref");

		for (Element child : DomUtils.getChildElements(element)) {
			builder.addPropertyValue("containerRef",
					parserContext.getDelegate().parseBeanDefinitionElement(child,
							builder.getRawBeanDefinition()));
		}

	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {
		String name = super.resolveId(element, definition, parserContext);
		if (!StringUtils.hasText(name)) {
			name = YarnSystemConstants.DEFAULT_ID_CONTAINER;
		}
		return name;
	}

}
