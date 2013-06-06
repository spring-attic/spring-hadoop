/*
 * Copyright 2006-2011 the original author or authors.
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
package org.springframework.data.hadoop.cascading.config;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.CustomEditorConfigurer;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.xml.NamespaceHandlerSupport;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

/**
 * Spring Cascading namespace handler
 *
 * @author Costin Leau
 * @author Thomas Risberg
 */
class CascadingNamespaceHandler extends NamespaceHandlerSupport {

	private static String DEFAULT_CONVERTER = "org.springframework.data.hadoop.mapreduce.MapReducePropertyEditorRegistrar.ns.registration";

	public void init() {
		registerBeanDefinitionParser("cascading-cascade", new CascadingCascadeParser());
		registerBeanDefinitionParser("cascading-flow", new CascadingFlowParser());
		registerBeanDefinitionParser("cascading-runner", new CascadingRunnerParser());
		registerBeanDefinitionParser("cascading-tasklet", new CascadingTaskletParser());
	}

	@Override
	public BeanDefinition parse(Element element, ParserContext parserContext) {
		// disable type conversion
		//registerImplicitBeans(parserContext);
		return super.parse(element, parserContext);
	}

	private void registerImplicitBeans(ParserContext parserContext) {
		BeanDefinitionRegistry registry = parserContext.getRegistry();
		if (!registry.containsBeanDefinition(DEFAULT_CONVERTER)) {
			BeanDefinition def = BeanDefinitionBuilder.genericBeanDefinition(CustomEditorConfigurer.class).setRole(
					BeanDefinition.ROLE_INFRASTRUCTURE).addPropertyValue(
					"propertyEditorRegistrars",
					BeanDefinitionBuilder.genericBeanDefinition(
							"org.springframework.data.hadoop.mapreduce.MapReducePropertyEditorRegistrar").
						setRole(BeanDefinition.ROLE_INFRASTRUCTURE).getBeanDefinition()).getBeanDefinition();
			registry.registerBeanDefinition(DEFAULT_CONVERTER, def);
		}
	}
}