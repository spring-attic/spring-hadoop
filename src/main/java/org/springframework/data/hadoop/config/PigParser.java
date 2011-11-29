/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.hadoop.config;

import java.util.Map;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.hadoop.pig.PigContextFactoryBean;
import org.springframework.data.hadoop.pig.PigServerFactoryBean;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;


/**
 * Parser for pig element.
 * 
 * @author Costin Leau
 */
class PigParser extends AbstractImprovedSimpleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return PigServerFactoryBean.class;
	}
	
	@Override
	protected boolean isEligibleAttribute(String attributeName) {
		return !("scripts".equals(attributeName) || "paths-to-skip".equals(attributeName)
				|| "job-tracker".equals(attributeName) || "configuration-ref".equals(attributeName) || "exec-type".equals(attributeName))
				&& super.isEligibleAttribute(attributeName);
	}


	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		// parse attributes using conventions
		super.doParse(element, parserContext, builder);

		// parse resources
		String attr = element.getAttribute("scripts");

		if (StringUtils.hasText(attr)) {
			builder.addPropertyValue("scripts", StringUtils.commaDelimitedListToStringArray(attr));
		}

		attr = element.getAttribute("paths-to-skip");
		if (StringUtils.hasText(attr)) {
			builder.addPropertyValue("pathsToSkip", StringUtils.commaDelimitedListToStringArray(attr));
		}

		// parse nested PigContext definition

		BeanDefinitionBuilder contextBuilder = BeanDefinitionBuilder.genericBeanDefinition(PigContextFactoryBean.class);

		Map parsedProps = parserContext.getDelegate().parsePropsElement(element);
		if (!parsedProps.isEmpty()) {
			contextBuilder.addPropertyValue("properties", parsedProps);
		}

		NamespaceUtils.setPropertyValue(element, contextBuilder, "job-tracker");
		NamespaceUtils.setPropertyValue(element, contextBuilder, "exec-type");
		NamespaceUtils.setPropertyReference(element, contextBuilder, "configuration-ref");

		builder.addPropertyValue("pigContext", contextBuilder.getBeanDefinition());
	}
	
	@Override
	protected String defaultId() {
		return "hadoop-pig";
	}
}