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

import java.util.List;

import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.xml.DomUtils;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;
import org.w3c.dom.Element;

/**
 * Simple namespace parser for yarn:localresources.
 *
 * @author Janne Valkealahti
 *
 */
public class LocalresourcesParser extends AbstractImprovedSimpleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return LocalResourcesFactoryBean.class;
	}

	@Override
	protected String defaultId(ParserContext context, Element element) {
		return YarnSystemConstants.DEFAULT_ID_LOCAL_RESOURCES;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		super.doParse(element, parserContext, builder);

		ManagedList<BeanDefinition> entries = new ManagedList<BeanDefinition>();
		parseTransferEntries(element, "hdfs", entries);
		builder.addPropertyValue("hdfsEntries", entries);

		entries = new ManagedList<BeanDefinition>();
		parseCopyEntries(element, "copy", entries);
		builder.addPropertyValue("copyEntries", entries);

		YarnNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "configuration");
	}

	private void parseTransferEntries(Element element, String name, List<BeanDefinition> entries) {
		List<Element> cp = DomUtils.getChildElementsByTagName(element, name);
		for (Element entry : cp) {
			BeanDefinitionBuilder bd = BeanDefinitionBuilder.genericBeanDefinition(TransferEntry.class);

			bd.addConstructorArgValue(
					entry.hasAttribute("type") ?
					LocalResourceType.valueOf(entry.getAttribute("type").toUpperCase()) :
					null);

			bd.addConstructorArgValue(
					entry.hasAttribute("visibility") ?
					LocalResourceVisibility.valueOf(entry.getAttribute("visibility").toUpperCase()) :
					null);

			bd.addConstructorArgValue(entry.getAttribute("path"));
			bd.addConstructorArgValue(entry.hasAttribute("staging") ? entry.getAttribute("staging") : false);
			entries.add(bd.getBeanDefinition());
		}
	}

	private void parseCopyEntries(Element element, String name, List<BeanDefinition> entries) {
		List<Element> cp = DomUtils.getChildElementsByTagName(element, name);
		for (Element entry : cp) {
			BeanDefinitionBuilder bd = BeanDefinitionBuilder.genericBeanDefinition(CopyEntry.class);
			bd.addConstructorArgValue(entry.getAttribute("src"));
			bd.addConstructorArgValue(entry.getAttribute("dest"));
			bd.addConstructorArgValue(entry.hasAttribute("staging") ? entry.getAttribute("staging") : false);
			entries.add(bd.getBeanDefinition());
		}
	}

}
