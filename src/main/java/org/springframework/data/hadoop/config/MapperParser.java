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

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.data.hadoop.mapreduce.MapperFactoryBean;
import org.w3c.dom.Element;

/**
 * 
 * Parser for {@link org.springframework.data.hadoop.mapreduce.MapperFactoryBean}
 * instances as defined in a &lt;mapper&gt; element.
 * 
 * @author Josh Long
 * @since 1.0
 * @see org.springframework.data.hadoop.mapreduce.MapperFactoryBean
 * @see org.springframework.data.hadoop.mapreduce.AutowiredJobFactoryBean
 * 
 */
class MapperParser extends AbstractSingleBeanDefinitionParser {
	/**
	 * the attribute for the 'target' bean to be used by the
	 * {@link MapperFactoryBean factory bean}
	 */
	public static final String REF_ATTR = "ref";

	/**
	 * attribute specifying method that is optionally used (on the bean
	 * {@link MapperParser#REF_ATTR references}) in lieu of a method with an
	 * annotation
	 * 
	 */
	public static final String METHOD_ATTR = "method";

	/**
	 * the attribute for the class of the key that's "emitted"
	 */
	public static final String OUTPUT_KEY_TYPE_ATTR = "output-key-type";

	/**
	 * the attribute for the class of the value that's "emitted"
	 */
	public static final String OUTPUT_VALUE_TYPE_ATTR = "output-value-type";

	@Override
	protected Class<?> getBeanClass(Element element) {
		return MapperFactoryBean.class;
	}

	@Override
	protected void doParse(Element element, BeanDefinitionBuilder builder) {
		builder.addPropertyValue("outputValueType", element.getAttribute(OUTPUT_VALUE_TYPE_ATTR));
		builder.addPropertyValue("outputKeyType", element.getAttribute(OUTPUT_KEY_TYPE_ATTR));
		builder.addPropertyReference("target", element.getAttribute(REF_ATTR));
		NamespaceUtils.setValueIfAttributeDefined(builder, element, METHOD_ATTR);
	}
}
