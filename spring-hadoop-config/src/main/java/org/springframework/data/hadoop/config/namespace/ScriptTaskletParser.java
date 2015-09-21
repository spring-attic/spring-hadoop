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

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.BeanDefinitionParserDelegate;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * Namespace parser for hdp:script.
 * 
 * @author Costin Leau
 */
class ScriptTaskletParser extends AbstractSimpleBeanDefinitionParser {


	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		super.doParse(element, parserContext, builder);

		// set scope
		String scope = element.getAttribute(BeanDefinitionParserDelegate.SCOPE_ATTRIBUTE);
		if (StringUtils.hasText(scope)) {
			builder.setScope(scope);
		}

		String attribute = element.getAttribute("script-ref");

		Element nestedScript = DomUtils.getChildElementByTagName(element, "script");
		if (StringUtils.hasText(attribute) && nestedScript != null) {
			parserContext.getReaderContext().error(
					"Cannot use define both 'script-ref' and a nested script; use only one", element);
		}

		Object script = null;
		if (nestedScript != null) {
			// parse the script definition
			BeanDefinition nested = new ScriptParser().parse(nestedScript, parserContext);
			if (StringUtils.hasText(scope)) {
				nested.setScope(scope);
			}
			script = nested;
		}
		else {
			script = new RuntimeBeanReference(attribute);
		}

		builder.addPropertyValue("scriptCallback", script);
	}


	@Override
	protected boolean shouldGenerateIdAsFallback() {
		return true;
	}

	@Override
	protected boolean isEligibleAttribute(String attributeName) {
		return false;
	}


	@Override
	protected String getBeanClassName(Element element) {
		return "org.springframework.data.hadoop.batch.scripting.ScriptTasklet";
	}
}