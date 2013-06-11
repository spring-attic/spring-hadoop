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

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * Base class for parsing bean definitions of type propertiesConfigured.
 *
 * @author Costin Leau
 */
abstract class AbstractPropertiesConfiguredBeanDefinitionParser extends AbstractImprovedSimpleBeanDefinitionParser {

	protected boolean isEligibleAttribute(String attributeName) {
		return !("properties-ref".equals(attributeName) || "properties-location".equals(attributeName))
				&& super.isEligibleAttribute(attributeName);
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		// parse attributes using conventions
		super.doParse(element, parserContext, builder);

		boolean hasProperties = false;

		BeanDefinitionBuilder b = BeanDefinitionBuilder.genericBeanDefinition(PropertiesFactoryBean.class);
		b.setScope(builder.getRawBeanDefinition().getScope());
		b.addPropertyValue("localOverride", true);
		ManagedList<Object> propsArray = new ManagedList<Object>(2);
		b.addPropertyValue("propertiesArray", propsArray);

		String attr = element.getAttribute("properties-ref");
		if (StringUtils.hasText(attr)) {
			hasProperties = true;
			propsArray.add(new RuntimeBeanReference(attr));
		}

		hasProperties |= NamespaceUtils.setCSVProperty(element, b, "properties-location", "locations");

		// parse nested properties
		attr = DomUtils.getTextValue(element);
		if (StringUtils.hasText(attr)) {
			hasProperties = true;
			propsArray.add(attr);
		}

		if (hasProperties) {
			b.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);
			builder.addPropertyValue("properties", b.getBeanDefinition());
		}
	}
}
