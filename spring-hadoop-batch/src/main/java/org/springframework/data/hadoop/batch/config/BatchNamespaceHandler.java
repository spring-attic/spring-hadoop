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
package org.springframework.data.hadoop.batch.config;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 * Core Spring Hadoop tasklet namespace handler
 *
 * @author Costin Leau
 * @author Thomas Risberg
 */
class BatchNamespaceHandler extends NamespaceHandlerSupport {

	public void init() {
		registerBeanDefinitionParser("job-tasklet", new HadoopJobTaskletParser());
		registerBeanDefinitionParser("tool-tasklet", new ToolTaskletParser());
		registerBeanDefinitionParser("jar-tasklet", new JarTaskletParser());
		registerBeanDefinitionParser("script-tasklet", new ScriptTaskletParser());
		registerBeanDefinitionParser("hive-tasklet", new HiveTaskletParser());
		registerBeanDefinitionParser("pig-tasklet", new PigTaskletParser());
	}

}