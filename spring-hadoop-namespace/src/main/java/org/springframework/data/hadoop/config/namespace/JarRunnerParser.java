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

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.hadoop.mapreduce.JarRunner;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * Parser for 'jar-runner' element.
 * 
 * @author Costin Leau
 */
public class JarRunnerParser extends AbstractGenericOptionsParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return JarRunner.class;
	}

	@Override
	protected boolean isEligibleAttribute(String attributeName) {
		return !("pre-action".equals(attributeName) || "post-action".equals(attributeName))
				&& super.isEligibleAttribute(attributeName);
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		super.doParse(element, parserContext, builder);
		parseJarDefinition(element, parserContext, builder);
	}

	public static void parseJarDefinition(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		List<Element> list = DomUtils.getChildElements(element);
		
		ManagedList<Object> args = new ManagedList<Object>();

		for (Element child : list) {
			if ("arg".equals(child.getLocalName())) {
				args.add(child.getAttribute("value").trim());
			}
		}
		
		builder.addPropertyValue("arguments", args);

		NamespaceUtils.setCSVReferenceProperty(element, builder, "pre-action", "preAction");
		NamespaceUtils.setCSVReferenceProperty(element, builder, "post-action", "postAction");
	}

	@Override
	protected boolean shouldGenerateIdAsFallback() {
		return true;
	}
}
