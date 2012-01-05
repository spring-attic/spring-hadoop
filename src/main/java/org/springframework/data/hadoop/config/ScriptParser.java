/*
 * Copyright 2011-2012 the original author or authors.
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

import java.util.List;

import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.hadoop.scripting.HdfsScriptFactoryBean;
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
class ScriptParser extends AbstractSimpleBeanDefinitionParser {



	@Override
	protected boolean isEligibleAttribute(String attributeName) {
		return !("source".equals(attributeName)) && super.isEligibleAttribute(attributeName);
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		// parse attributes using conventions
		super.doParse(element, parserContext, builder);

		// parse source
		String source = element.getAttribute("source");
		Element inline = DomUtils.getChildElementByTagName(element, "inline-script");

		Object scriptSource = null;

		if (StringUtils.hasText(source)) {
			if (inline != null) {
				parserContext.getReaderContext().error(
						"cannot specify both 'source' and 'inline-script'; use only one", element);
			}

			BeanDefinitionBuilder b = BeanDefinitionBuilder.genericBeanDefinition(ResourceScriptSource.class);
			b.addConstructorArgValue(source);
			scriptSource = b.getBeanDefinition();
		}
		else {
			if (inline == null) {
				parserContext.getReaderContext().error("no 'source' or 'inline-script' specified", element);
			}

			scriptSource = new StaticScriptSource(DomUtils.getTextValue(inline), element.getAttribute("id"));
		}

		builder.addPropertyValue("scriptSource", scriptSource);

		// no language specified, figure out from the source
		if (!element.hasAttribute("language")) {
			if (inline != null) {
				parserContext.getReaderContext().error("the language needs to be specified when using 'inline-script'", element);
			}

			builder.addPropertyValue("extension", StringUtils.getFilenameExtension(source));
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
		return HdfsScriptFactoryBean.class;
	}

	@Override
	protected boolean shouldGenerateIdAsFallback() {
		return true;
	}
}