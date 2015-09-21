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
package org.springframework.data.hadoop.config.namespace;

import java.util.Collection;
import java.util.Collections;

import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.pig.PigContextFactoryBean;
import org.springframework.data.hadoop.pig.PigScript;
import org.springframework.data.hadoop.pig.PigServerFactoryBean;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;


/**
 * Parser for 'pig-server' element.
 *
 * @author Costin Leau
 */
public class PigServerParser extends AbstractPropertiesConfiguredBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return PigServerFactoryBean.class;
	}

	@Override
	protected boolean isEligibleAttribute(String attributeName) {
		return !("paths-to-skip".equals(attributeName)
				|| "job-tracker".equals(attributeName) || "configuration-ref".equals(attributeName) || "exec-type".equals(attributeName))
				&& super.isEligibleAttribute(attributeName);
	}

	@Override
	protected String defaultId(ParserContext context, Element element) {
		return "pigFactory";
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		// parse attributes using conventions
		super.doParse(element, parserContext, builder);

		// parse scripts
		Collection<BeanDefinition> scripts = parseScripts(parserContext, element);
		if (!CollectionUtils.isEmpty(scripts)) {
			builder.addPropertyValue("scripts", scripts);
		}

		NamespaceUtils.setCSVProperty(element, builder, "paths-to-skip", "pathsToSkip");

		// parse nested PigContext definition
		BeanDefinitionBuilder contextBuilder = BeanDefinitionBuilder.genericBeanDefinition(PigContextFactoryBean.class);

		NamespaceUtils.setPropertyValue(element, contextBuilder, "job-tracker");
		NamespaceUtils.setPropertyValue(element, contextBuilder, "exec-type");
		NamespaceUtils.setPropertyReference(element, contextBuilder, "configuration-ref");

		// move properties setting from PigServer to PigContext class
		MutablePropertyValues pv = builder.getRawBeanDefinition().getPropertyValues();
		String prop = "properties";
		if (pv.contains(prop)) {
			contextBuilder.addPropertyValue(prop, pv.getPropertyValue(prop).getValue());
			pv.removePropertyValue(prop);
		}

		builder.addPropertyValue("pigContext", contextBuilder.getBeanDefinition());
	}

	public static Collection<BeanDefinition> parseScripts(ParserContext context, Element element) {
		Collection<Element> children = DomUtils.getChildElementsByTagName(element, "script");

		if (!children.isEmpty()) {
			Collection<BeanDefinition> defs = new ManagedList<BeanDefinition>(children.size());

			for (Element child : children) {
				// parse source
				String location = child.getAttribute("location");
				String inline = DomUtils.getTextValue(child);
				boolean hasScriptInlined = StringUtils.hasText(inline);


				GenericBeanDefinition def = new GenericBeanDefinition();
				def.setSource(child);
				def.setBeanClass(PigScript.class);

				Object resource = null;

				if (StringUtils.hasText(location)) {
					if (hasScriptInlined) {
						context.getReaderContext().error("cannot specify both 'location' and a nested script; use only one", element);
					}
					resource = location;
				}
				else {
					if (!hasScriptInlined) {
						context.getReaderContext().error("no 'location' or nested script specified", element);
					}

					resource = BeanDefinitionBuilder.genericBeanDefinition(ByteArrayResource.class).
							addConstructorArgValue(inline).
							addConstructorArgValue("resource for inlined script").getBeanDefinition();
				}

				def.getConstructorArgumentValues().addIndexedArgumentValue(0, resource, Resource.class.getName());

				String args = DomUtils.getChildElementValueByTagName(child, "arguments");

				if (args != null) {
					// create linked properties
					BeanDefinition params = BeanDefinitionBuilder.genericBeanDefinition(LinkedProperties.class).addConstructorArgValue(args).getBeanDefinition();
					def.getConstructorArgumentValues().addIndexedArgumentValue(1, params);
				}
				defs.add(def);
			}

			return defs;
		}

		return Collections.emptyList();
	}
}