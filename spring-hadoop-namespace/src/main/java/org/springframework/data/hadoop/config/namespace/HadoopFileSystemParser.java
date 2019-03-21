/*
 * Copyright 2011-2013 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      https://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.config.namespace;

import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.hadoop.fs.FileSystemFactoryBean;
import org.w3c.dom.Element;

/**
 * @author Costin Leau
 */
class HadoopFileSystemParser extends AbstractImprovedSimpleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return FileSystemFactoryBean.class;
	}

	@Override
	protected String defaultId(ParserContext context, Element element) {
		return "hadoopFs";
	}
}
