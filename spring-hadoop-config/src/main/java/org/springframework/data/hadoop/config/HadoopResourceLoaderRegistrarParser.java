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
package org.springframework.data.hadoop.config;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.hadoop.HadoopSystemConstants;
import org.springframework.data.hadoop.fs.CustomResourceLoaderRegistrar;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * Namespace parser for 'resource-loader-registrar' element.
 *
 * @author Janne Valkealahti
 *
 */
public class HadoopResourceLoaderRegistrarParser extends AbstractSingleBeanDefinitionParser {

	@Override
	protected Class<CustomResourceLoaderRegistrar> getBeanClass(Element element) {
		return CustomResourceLoaderRegistrar.class;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		String loaderValue = element.getAttribute("loader-ref");
		if (!StringUtils.hasText(loaderValue)) {
			loaderValue = HadoopSystemConstants.DEFAULT_ID_RESOURCE_LOADER;
		}
		builder.addPropertyReference("loader", loaderValue);
	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {
		String name = super.resolveId(element, definition, parserContext);
		if (!StringUtils.hasText(name)) {
			name = HadoopSystemConstants.DEFAULT_ID_RESOURCE_LOADER_REGISTRAR;
		}
		return name;
	}

}
