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
//import java.util.Collections;
//
//import org.springframework.beans.factory.config.BeanDefinition;
//import org.springframework.beans.factory.support.BeanDefinitionBuilder;
//import org.springframework.beans.factory.support.GenericBeanDefinition;
//import org.springframework.beans.factory.support.ManagedList;
//import org.springframework.beans.factory.xml.ParserContext;
//import org.springframework.core.io.ByteArrayResource;
//import org.springframework.core.io.Resource;
//import org.springframework.data.hadoop.hive.HiveRunner;
//import org.springframework.data.hadoop.hive.HiveScript;
//import org.springframework.util.CollectionUtils;
//import org.springframework.util.StringUtils;
//import org.springframework.util.xml.DomUtils;
//import org.w3c.dom.Element;
//
///**
// * @author Costin Leau
// */
//public class HiveRunnerParser extends AbstractImprovedSimpleBeanDefinitionParser {
//
//	@Override
//	protected Class<?> getBeanClass(Element element) {
//		return HiveRunner.class;
//	}
//
//	@Override
//	protected boolean isEligibleAttribute(String attributeName) {
//		return !("pre-action".equals(attributeName) || "post-action".equals(attributeName))
//				&& super.isEligibleAttribute(attributeName);
//	}
//
//	@Override
//	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
//		// parse attributes using conventions
//		super.doParse(element, parserContext, builder);
//
//		NamespaceUtils.setCSVReferenceProperty(element, builder, "pre-action", "preAction");
//		NamespaceUtils.setCSVReferenceProperty(element, builder, "post-action", "postAction");
//
//		// parse scripts
//		Collection<Object> scripts = parseScripts(parserContext, element);
//		if (!CollectionUtils.isEmpty(scripts)) {
//			builder.addPropertyValue("scripts", scripts);
//		}
//	}
//
//    public static Collection<Object> parseScripts(ParserContext context, Element element) {
//        Collection<Element> children = DomUtils.getChildElementsByTagName(element, "script");
//
//        if (!children.isEmpty()) {
//            Collection<Object> defs = new ManagedList<Object>(children.size());
//
//            for (Element child : children) {
//                // parse source
//                String location = child.getAttribute("location");
//                String inline = DomUtils.getTextValue(child);
//                boolean hasScriptInlined = StringUtils.hasText(inline);
//
//                GenericBeanDefinition def = new GenericBeanDefinition();
//                def.setSource(child);
//                def.setBeanClass(HiveScript.class);
//
//                Object resource = null;
//
//                if (StringUtils.hasText(location)) {
//                    if (hasScriptInlined) {
//                        context.getReaderContext().error("cannot specify both 'location' and a nested script; use only one", element);
//                    }
//                    resource = location;
//                }
//                else {
//                    if (!hasScriptInlined) {
//                        context.getReaderContext().error("no 'location' or nested script specified", element);
//                    }
//
//                    resource = BeanDefinitionBuilder.genericBeanDefinition(ByteArrayResource.class).
//                            addConstructorArgValue(inline).
//                            addConstructorArgValue("resource for inlined script").getBeanDefinition();
//                }
//
//                def.getConstructorArgumentValues().addIndexedArgumentValue(0, resource, Resource.class.getName());
//                String args = DomUtils.getChildElementValueByTagName(child, "arguments");
//
//                if (args != null) {
//                    BeanDefinition params = BeanDefinitionBuilder.genericBeanDefinition(LinkedProperties.class).addConstructorArgValue(args).getBeanDefinition();
//                    def.getConstructorArgumentValues().addIndexedArgumentValue(1, params);
//                }
//                defs.add(def);
//
//            }
//
//            return defs;
//        }
//
//        return Collections.emptyList();
//    }
//
//    @Override
//	protected boolean shouldGenerateIdAsFallback() {
//		return true;
//	}
//}
