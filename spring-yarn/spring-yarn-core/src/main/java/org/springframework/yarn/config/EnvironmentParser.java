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

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.xml.DomUtils;
import org.springframework.yarn.configuration.EnvironmentFactoryBean;
import org.springframework.yarn.support.ParsingUtils;
import org.w3c.dom.Element;

/**
 * Simple namespace parser for yarn:environment.
 *
 * @author Janne Valkealahti
 *
 */
class EnvironmentParser extends AbstractPropertiesConfiguredBeanDefinitionParser {

	public static final String DEFAULT_ID = "yarnEnvironment";

	@Override
	protected Class<?> getBeanClass(Element element) {
		return EnvironmentFactoryBean.class;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		super.doParse(element, parserContext, builder);

		builder.addPropertyValue("includeSystemEnv", element.getAttribute("include-system-env"));

		List<Element> entries = DomUtils.getChildElementsByTagName(element, "classpath");
		if(entries.size() == 1) {
			Element entry = entries.get(0);
			String textContent = entry.getTextContent();
			String defYarnClasspath = entry.getAttribute("default-yarn-app-classpath");
			String delimiter = entry.getAttribute("delimiter");
			builder.addPropertyValue("defaultYarnAppClasspath", defYarnClasspath);
			builder.addPropertyValue("delimiter", delimiter);
			// nested entries will be added to classpath
			builder.addPropertyValue("classpath", ParsingUtils.extractClasspath(textContent, delimiter));
		} else if (entries.size() > 1) {
			parserContext.getReaderContext().error("only one nested <classpath> element allowed under <environment>", element);
		}
	}

	@Override
	protected String defaultId(ParserContext context, Element element) {
		return DEFAULT_ID;
	}

}
