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
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.StaticAppmaster;
import org.springframework.yarn.am.StaticEventingAppmaster;
import org.springframework.yarn.am.allocate.DefaultContainerAllocator;
import org.springframework.yarn.am.container.DefaultContainerLauncher;
import org.springframework.yarn.am.monitor.DefaultContainerMonitor;
import org.springframework.yarn.container.CommandLineContainerRunner;
import org.springframework.yarn.launch.LaunchCommandsFactoryBean;
import org.springframework.yarn.support.ParsingUtils;
import org.w3c.dom.Element;

/**
 * Simple namespace parser for yarn:master.
 *
 * @author Janne Valkealahti
 *
 */
public class MasterParser extends AbstractBeanDefinitionParser {

	@Override
	protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {

		// for now, defaulting to StaticAppmaster
		// and checking if implementation class was defined
		String type = element.getAttribute("type");
		String clazz = element.getAttribute("appmaster-class");

		BeanDefinitionBuilder builder;
		if (StringUtils.hasText(clazz)) {
			builder = BeanDefinitionBuilder.genericBeanDefinition(clazz);
		} else if (type.equals("event")) {
			builder = BeanDefinitionBuilder.genericBeanDefinition(StaticEventingAppmaster.class);
		} else {
			builder = BeanDefinitionBuilder.genericBeanDefinition(StaticAppmaster.class);
		}

		// parsing command needed for master
		Element containerCommandElement = DomUtils.getChildElementByTagName(element, "container-command");
		if(containerCommandElement != null) {
			String textContent = containerCommandElement.getTextContent();
			String command = ParsingUtils.extractRunnableCommand(textContent);
			List<String> commands = new ArrayList<String>();
			commands.add(command);
			builder.addPropertyValue("commands", commands);
		}

		// build commands from master-runner element
		Element containerRunnerElement = DomUtils.getChildElementByTagName(element, "container-runner");
		if(containerRunnerElement != null && containerCommandElement == null) {
			BeanDefinitionBuilder defBuilder = BeanDefinitionBuilder.genericBeanDefinition(LaunchCommandsFactoryBean.class);
			YarnNamespaceUtils.setValueIfAttributeDefined(defBuilder, containerRunnerElement, "command");
			if (containerRunnerElement.hasAttribute("runner")) {
				defBuilder.addPropertyValue("runner", containerRunnerElement.getAttribute("runner"));
			} else {
				defBuilder.addPropertyValue("runner", CommandLineContainerRunner.class);
			}
			YarnNamespaceUtils.setValueIfAttributeDefined(defBuilder, containerRunnerElement, "context-file", false, "container-context.xml");
			YarnNamespaceUtils.setValueIfAttributeDefined(defBuilder, containerRunnerElement, "bean-name", false, YarnSystemConstants.DEFAULT_ID_CONTAINER);
			YarnNamespaceUtils.setReferenceIfAttributeDefined(defBuilder, containerRunnerElement, "arguments");
			YarnNamespaceUtils.setValueIfAttributeDefined(defBuilder, containerRunnerElement, "stdout", false, "<LOG_DIR>/Container.stdout");
			YarnNamespaceUtils.setValueIfAttributeDefined(defBuilder, containerRunnerElement, "stderr", false, "<LOG_DIR>/Container.stderr");
			AbstractBeanDefinition beanDef = defBuilder.getBeanDefinition();
			String beanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
			parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, beanName));
			builder.addPropertyReference("commands", beanName);
		}

		// allocator - for now, defaulting to DefaultContainerAllocator
		BeanDefinitionBuilder defBuilder = BeanDefinitionBuilder.genericBeanDefinition(DefaultContainerAllocator.class);
		defBuilder.addPropertyReference("configuration", YarnSystemConstants.DEFAULT_ID_CONFIGURATION);
		Element allocElement = DomUtils.getChildElementByTagName(element, "container-allocator");
		YarnNamespaceUtils.setReferenceIfAttributeDefined(defBuilder, element, "environment", YarnSystemConstants.DEFAULT_ID_ENVIRONMENT);
		if(allocElement != null) {
			YarnNamespaceUtils.setValueIfAttributeDefined(defBuilder, allocElement, "virtualcores");
			YarnNamespaceUtils.setValueIfAttributeDefined(defBuilder, allocElement, "memory");
			YarnNamespaceUtils.setValueIfAttributeDefined(defBuilder, allocElement, "priority");
			YarnNamespaceUtils.setValueIfAttributeDefined(defBuilder, allocElement, "locality");
		}
		AbstractBeanDefinition beanDef = defBuilder.getBeanDefinition();
		String beanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
		parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, beanName));
		builder.addPropertyReference("allocator", beanName);

		// launcher - for now, defaulting to DefaultContainerLauncher
		defBuilder = BeanDefinitionBuilder.genericBeanDefinition(DefaultContainerLauncher.class);
		YarnNamespaceUtils.setReferenceIfAttributeDefined(defBuilder, element, "configuration", YarnSystemConstants.DEFAULT_ID_CONFIGURATION);
		YarnNamespaceUtils.setReferenceIfAttributeDefined(defBuilder, element, "environment", YarnSystemConstants.DEFAULT_ID_ENVIRONMENT);
		YarnNamespaceUtils.setReferenceIfAttributeDefined(defBuilder, element, "resource-localizer", YarnSystemConstants.DEFAULT_ID_LOCAL_RESOURCES);
		beanDef = defBuilder.getBeanDefinition();
		beanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
		parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, beanName));
		builder.addPropertyReference("launcher", beanName);

		// monitor - for now, defaulting to DefaultContainerMonitor
		defBuilder = BeanDefinitionBuilder.genericBeanDefinition(DefaultContainerMonitor.class);
		beanDef = defBuilder.getBeanDefinition();
		beanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
		parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, beanName));
		builder.addPropertyReference("monitor", beanName);

		// for appmaster bean
		YarnNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "resource-localizer", YarnSystemConstants.DEFAULT_ID_LOCAL_RESOURCES);
		YarnNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "configuration", YarnSystemConstants.DEFAULT_ID_CONFIGURATION);

		return builder.getBeanDefinition();
	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {
		String name = super.resolveId(element, definition, parserContext);
		if (!StringUtils.hasText(name)) {
			name = YarnSystemConstants.DEFAULT_ID_APPMASTER;
		}
		return name;
	}

}
