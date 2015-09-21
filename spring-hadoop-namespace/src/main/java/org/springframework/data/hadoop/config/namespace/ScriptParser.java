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

import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.BeanDefinitionParserDelegate;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.hadoop.scripting.HdfsScriptRunner;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.scripting.support.StaticScriptSource;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * Namespace parser for hdp:script.
 * 
 * @author Costin Leau
 */
public class ScriptParser extends AbstractImprovedSimpleBeanDefinitionParser {


	@Override
	protected boolean isEligibleAttribute(String attributeName) {
		return !("location".equals(attributeName) || "pre-action".equals(attributeName) || "post-action".equals(attributeName))
				&& super.isEligibleAttribute(attributeName);
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		// parse attributes using conventions
		super.doParse(element, parserContext, builder);

		NamespaceUtils.setCSVProperty(element, builder, "pre-action", "preAction");
		NamespaceUtils.setCSVProperty(element, builder, "post-action", "postAction");

		// set scope
		String scope = element.getAttribute(BeanDefinitionParserDelegate.SCOPE_ATTRIBUTE);
		if (StringUtils.hasText(scope)) {
			builder.setScope(scope);
		}

		// parse source
		String location = element.getAttribute("location");
		String inline = DomUtils.getTextValue(element);
		boolean hasScriptInlined = StringUtils.hasText(inline);

		Object scriptSource = null;

		if (StringUtils.hasText(location)) {
			if (hasScriptInlined) {
				parserContext.getReaderContext().error("cannot specify both 'location' and a nested script; use only one", element);
			}

			BeanDefinitionBuilder b = BeanDefinitionBuilder.genericBeanDefinition(ResourceScriptSource.class);
			b.addConstructorArgValue(location);
			scriptSource = b.getBeanDefinition();
		}
		else {
			if (!hasScriptInlined) {
				parserContext.getReaderContext().error("no 'location' or nested script specified", element);
			}

			BeanDefinitionBuilder b = BeanDefinitionBuilder.genericBeanDefinition(StaticScriptSource.class);
			b.addConstructorArgValue(inline);
			b.addConstructorArgValue(element.getAttribute("id"));

			scriptSource = b.getBeanDefinition();
		}

		builder.addPropertyValue("scriptSource", scriptSource);

		// no language specified, figure out from the source
		if (!element.hasAttribute("language")) {
			if (hasScriptInlined) {
				parserContext.getReaderContext().error("the language needs to be specified when using an inlined script", element);
			}

			builder.addPropertyValue("extension", StringUtils.getFilenameExtension(location));
		}

		// parse properties
		BeanDefinition bd = new GenericBeanDefinition();
		parserContext.getDelegate().parsePropertyElements(element, bd);

		ManagedMap<String, Object> args = new ManagedMap<String, Object>();
		List<PropertyValue> list = bd.getPropertyValues().getPropertyValueList();

		for (PropertyValue pv : list) {
			args.put(pv.getName(), pv.getValue());
		}

		builder.addPropertyValue("arguments", args);
	}

	@Override
	protected Class<?> getBeanClass(Element element) {
		return HdfsScriptRunner.class;
	}

	@Override
	protected boolean shouldGenerateIdAsFallback() {
		return true;
	}
}