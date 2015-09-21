/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.hadoop.config.namespace;

import java.util.List;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.hadoop.fs.DistributedCacheFactoryBean;
import org.springframework.data.hadoop.fs.DistributedCacheFactoryBean.CacheEntry;
import org.springframework.data.hadoop.fs.DistributedCacheFactoryBean.CacheEntry.EntryType;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * Parser for 'distributed-cache' element.
 * 
 * @author Costin Leau
 */
class DistributedCacheParser extends AbstractImprovedSimpleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return DistributedCacheFactoryBean.class;
	}

	@Override
	protected String defaultId(ParserContext context, Element element) {
		return "hadoopCache";
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		// parse attributes using conventions
		super.doParse(element, parserContext, builder);

		ManagedList<BeanDefinition> entries = new ManagedList<BeanDefinition>();

		parseEntries(element, "classpath", EntryType.CP, entries);
		parseEntries(element, "cache", EntryType.CACHE, entries);
		parseEntries(element, "local", EntryType.LOCAL, entries);

		builder.addPropertyValue("entries", entries);
	}

	private void parseEntries(Element element, String name, EntryType type, List<BeanDefinition> entries) {
		List<Element> cp = DomUtils.getChildElementsByTagName(element, name);

		for (Element entry : cp) {
			BeanDefinitionBuilder bd = BeanDefinitionBuilder.genericBeanDefinition(CacheEntry.class);
			bd.addConstructorArgValue(type);
			bd.addConstructorArgValue(entry.getAttribute("value"));
			entries.add(bd.getBeanDefinition());
		}
	}
}
