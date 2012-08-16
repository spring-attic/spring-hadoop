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
package org.springframework.data.hadoop.config;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.CustomEditorConfigurer;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.xml.NamespaceHandlerSupport;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.hadoop.mapreduce.MapReducePropertyEditorRegistrar;
import org.w3c.dom.Element;

/**
 * Core Spring Hadoop namespace handler
 *
 * @author Costin Leau
 */
class HadoopNamespaceHandler extends NamespaceHandlerSupport {

	private static String DEFAULT_CONVERTER = MapReducePropertyEditorRegistrar.class.getName() + ".ns.registration";

	public void init() {
		registerBeanDefinitionParser("job-tasklet", new HadoopTaskletParser());
		registerBeanDefinitionParser("job", new HadoopJobParser());
		registerBeanDefinitionParser("streaming", new HadoopStreamJobParser());
		registerBeanDefinitionParser("configuration", new HadoopConfigParser());
		registerBeanDefinitionParser("file-system", new HadoopFileSystemParser());
		registerBeanDefinitionParser("resource-loader", new HadoopResourceLoaderParser());
		registerBeanDefinitionParser("cache", new DistributedCacheParser());

		registerBeanDefinitionParser("tool-runner", new ToolRunnerParser());
		registerBeanDefinitionParser("tool-tasklet", new ToolTaskletParser());

		registerBeanDefinitionParser("jar-runner", new JarRunnerParser());
		registerBeanDefinitionParser("jar-tasklet", new JarTaskletParser());

		registerBeanDefinitionParser("script", new ScriptParser());
		registerBeanDefinitionParser("script-tasklet", new ScriptTaskletParser());

		registerBeanDefinitionParser("pig", new PigServerParser());
		registerBeanDefinitionParser("pig-tasklet", new PigTaskletParser());

		registerBeanDefinitionParser("hive-client", new HiveClientParser());
		registerBeanDefinitionParser("hive-server", new HiveServerParser());
		registerBeanDefinitionParser("hive-tasklet", new HiveTaskletParser());

		registerBeanDefinitionParser("hbase-configuration", new HbaseConfigurationParser());
	}

	@Override
	public BeanDefinition parse(Element element, ParserContext parserContext) {
		registerImplicitBeans(parserContext);
		return super.parse(element, parserContext);
	}

	private void registerImplicitBeans(ParserContext parserContext) {
		BeanDefinitionRegistry registry = parserContext.getRegistry();
		if (!registry.containsBeanDefinition(DEFAULT_CONVERTER)) {
			BeanDefinition def = BeanDefinitionBuilder.genericBeanDefinition(CustomEditorConfigurer.class).setRole(
					BeanDefinition.ROLE_INFRASTRUCTURE).addPropertyValue(
					"propertyEditorRegistrars",
					BeanDefinitionBuilder.genericBeanDefinition(MapReducePropertyEditorRegistrar.class).
						setRole(BeanDefinition.ROLE_INFRASTRUCTURE).getBeanDefinition()).getBeanDefinition();
			registry.registerBeanDefinition(DEFAULT_CONVERTER, def);
		}
	}
}