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

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.CommandLineAppmasterRunner;
import org.springframework.yarn.client.YarnClientFactoryBean;
import org.springframework.yarn.launch.LaunchCommandsFactoryBean;
import org.springframework.yarn.support.ParsingUtils;
import org.w3c.dom.Element;

/**
 * Simple namespace parser for yarn:client.
 *
 * @author Janne Valkealahti
 *
 */
public class ClientParser extends AbstractSingleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return YarnClientFactoryBean.class;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		YarnNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "template");
		YarnNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "configuration", YarnSystemConstants.DEFAULT_ID_CONFIGURATION);
		YarnNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "environment", YarnSystemConstants.DEFAULT_ID_ENVIRONMENT);
		YarnNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "resource-localizer", YarnSystemConstants.DEFAULT_ID_LOCAL_RESOURCES);
		YarnNamespaceUtils.setValueIfAttributeDefined(builder, element, "app-name");
		YarnNamespaceUtils.setValueIfAttributeDefined(builder, element, "priority");
		YarnNamespaceUtils.setValueIfAttributeDefined(builder, element, "virtualcores");
		YarnNamespaceUtils.setValueIfAttributeDefined(builder, element, "memory");
		YarnNamespaceUtils.setValueIfAttributeDefined(builder, element, "queue");

		// parsing command needed for master
		Element masterCommandElement = DomUtils.getChildElementByTagName(element, "master-command");
		if(masterCommandElement != null) {
			String textContent = masterCommandElement.getTextContent();
			String command = ParsingUtils.extractRunnableCommand(textContent);
			List<String> commands = new ArrayList<String>();
			commands.add(command);
			builder.addPropertyValue("commands", commands);
		}

		// build commands from master-runner element
		Element masterRunnerElement = DomUtils.getChildElementByTagName(element, "master-runner");
		if(masterRunnerElement != null && masterCommandElement == null) {
			BeanDefinitionBuilder defBuilder = BeanDefinitionBuilder.genericBeanDefinition(LaunchCommandsFactoryBean.class);
			YarnNamespaceUtils.setValueIfAttributeDefined(defBuilder, masterRunnerElement, "command");
			if (masterRunnerElement.hasAttribute("runner")) {
				defBuilder.addPropertyValue("runner", masterRunnerElement.getAttribute("runner"));								
			} else {
				defBuilder.addPropertyValue("runner", CommandLineAppmasterRunner.class);				
			}
			YarnNamespaceUtils.setValueIfAttributeDefined(defBuilder, masterRunnerElement, "context-file", false, "appmaster-context.xml");
			YarnNamespaceUtils.setValueIfAttributeDefined(defBuilder, masterRunnerElement, "bean-name", false, YarnSystemConstants.DEFAULT_ID_APPMASTER);
			YarnNamespaceUtils.setReferenceIfAttributeDefined(defBuilder, masterRunnerElement, "arguments");
			YarnNamespaceUtils.setValueIfAttributeDefined(defBuilder, masterRunnerElement, "stdout", false, "<LOG_DIR>/Appmaster.stdout");
			YarnNamespaceUtils.setValueIfAttributeDefined(defBuilder, masterRunnerElement, "stderr", false, "<LOG_DIR>/Appmaster.stderr");
			AbstractBeanDefinition beanDef = defBuilder.getBeanDefinition();
			String beanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
			parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, beanName));
			builder.addPropertyReference("commands", beanName);
		}
	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {
		String name = super.resolveId(element, definition, parserContext);
		if (!StringUtils.hasText(name)) {
			name = YarnSystemConstants.DEFAULT_ID_CLIENT;
		}
		return name;
	}

}
