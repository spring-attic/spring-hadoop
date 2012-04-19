/*
 * Copyright 2011-2012 the original author or authors.
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

import java.util.concurrent.Callable;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.BeanDefinitionParserDelegate;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.hadoop.scripting.HdfsScriptFactoryBean;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * Namespace parser for hdp:script.
 * 
 * @author Costin Leau
 */
class ScriptTaskletParser extends AbstractSimpleBeanDefinitionParser {


	/**
	 * Internal class used for wrapping a Hdfs script FB into a callable class to postpone the actual script evaluation.
	 * Useful for nesting scripts as nested bean while preventing the FactoryBean from being evaluated.
	 * 
	 * @author Costin Leau
	 */
	private static class ScriptFactoryBeanCallable extends HdfsScriptFactoryBean {


		private Object accessibleGetObject() {
			return super.getObject();
		}

		@Override
		public Object getObject() {
			return new Callable<Object>() {
				@Override
				public Object call() throws Exception {
					return accessibleGetObject();
				}
			};
		}
	}

	private static class ScriptBeanReference implements Callable<Object>, ApplicationContextAware {

		private String beanName;
		private ApplicationContext ctx;


		@Override
		public Object call() throws Exception {
			return ctx.getBean(beanName);
		}

		@Override
		public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
			this.ctx = applicationContext;
		}

		public void setBeanName(String beanName) {
			this.beanName = beanName;
		}
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		super.doParse(element, parserContext, builder);

		// set scope
		String scope = element.getAttribute(BeanDefinitionParserDelegate.SCOPE_ATTRIBUTE);
		if (StringUtils.hasText(scope)) {
			builder.setScope(scope);
		}

		String attribute = element.getAttribute("script-ref");

		Element nestedScript = DomUtils.getChildElementByTagName(element, "script");
		if (StringUtils.hasText(attribute) && nestedScript != null) {
			parserContext.getReaderContext().error("Cannot use define both 'script-ref' and a nested script; use only one", element);
		}

		if (nestedScript != null) {
			// parse the script definition
			BeanDefinition parse = new ScriptParser().parse(nestedScript, parserContext);
			// ugly but we know the type (as the class is private and under our control)
			((AbstractBeanDefinition) parse).setBeanClass(ScriptFactoryBeanCallable.class);
			if (StringUtils.hasText(scope)) {
				parse.setScope(scope);
			}
			builder.addPropertyValue("scriptCallback", parse);
		}
		else {
			BeanDefinitionBuilder nBuilder = BeanDefinitionBuilder.genericBeanDefinition(ScriptBeanReference.class);
			nBuilder.addPropertyValue("beanName", attribute);
			builder.addPropertyValue("scriptCallback", nBuilder.getBeanDefinition());
		}
	}


	@Override
	protected boolean shouldGenerateIdAsFallback() {
		return false;
	}

	@Override
	protected boolean isEligibleAttribute(String attributeName) {
		return false;
	}


	@Override
	protected String getBeanClassName(Element element) {
		return "org.springframework.data.hadoop.scripting.ScriptTasklet";
	}
}