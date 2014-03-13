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
package org.springframework.data.hadoop.config;

import java.util.Collection;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.hadoop.batch.hive.HiveTasklet;
import org.springframework.util.CollectionUtils;
import org.w3c.dom.Element;

/**
 * Parser for 'hive-tasklet' element.
 *
 * @author Costin Leau
 */
class HiveTaskletParser extends AbstractImprovedSimpleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return HiveTasklet.class;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		// parse attributes using conventions
		super.doParse(element, parserContext, builder);

		// parse scripts
		Collection<Object> scripts = HiveRunnerParser.parseScripts(parserContext, element);
		if (!CollectionUtils.isEmpty(scripts)) {
			builder.addPropertyValue("scripts", scripts);
		}
	}

	@Override
	protected boolean shouldGenerateIdAsFallback() {
		return true;
	}
}