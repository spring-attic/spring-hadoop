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

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.hadoop.configuration.StreamJobFactoryBean;
import org.w3c.dom.Element;

/**
 * Job declaration parser.
 * 
 * @author Costin Leau
 */
class HadoopStreamJobParser extends AbstractSimpleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return StreamJobFactoryBean.class;
	}

	@Override
	protected boolean isEligibleAttribute(String attributeName) {
		boolean eligible = super.isEligibleAttribute(attributeName);

		if (eligible) {
			return !"configuration-ref".equals(attributeName);
		}

		return false;

	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		super.doParse(element, parserContext, builder);

		String configRef = element.getAttribute("configuration-ref");
		builder.addPropertyReference("configuration", configRef);
	}
}
