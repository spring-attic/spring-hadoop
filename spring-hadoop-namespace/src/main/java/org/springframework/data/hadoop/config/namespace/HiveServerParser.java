/*
// * Copyright 2011-2013 the original author or authors.
// * 
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// * 
// *      http://www.apache.org/licenses/LICENSE-2.0
// * 
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.springframework.data.hadoop.config.namespace;
//
//import java.util.Collection;
//
//import org.springframework.beans.factory.support.BeanDefinitionBuilder;
//import org.springframework.beans.factory.xml.ParserContext;
//import org.springframework.data.hadoop.hive.HiveServerFactoryBean;
//import org.springframework.util.CollectionUtils;
//import org.springframework.util.StringUtils;
//import org.springframework.util.xml.DomUtils;
//import org.w3c.dom.Element;
//
///**
// * Parser for "hive-server" element.
// * 
// * @author Costin Leau
// */
//class HiveServerParser extends AbstractPropertiesConfiguredBeanDefinitionParser {
//
//	@Override
//	protected Class<?> getBeanClass(Element element) {
//		return HiveServerFactoryBean.class;
//	}
//
//	@Override
//	protected String defaultId(ParserContext context, Element element) {
//		return "hiveServer";
//	}
//
//	@Override
//	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
//		// parse attributes using conventions
//		super.doParse(element, parserContext, builder);
//
//		// parse properties
//		String props = DomUtils.getTextValue(element);
//
//		if (StringUtils.hasText(props)) {
//			builder.addPropertyValue("properties", props);
//		}
//
//		// parse scripts
//		Collection<Object> scripts = HiveRunnerParser.parseScripts(parserContext, element);
//		if (!CollectionUtils.isEmpty(scripts)) {
//			builder.addPropertyValue("scripts", scripts);
//		}
//	}
//}