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

import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.core.Conventions;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * Namespace utilities.
 *
 * @author Costin Leau
 */
abstract class NamespaceUtils {

	static final String REF_ATTRIBUTE = "ref";

	static boolean isReference(String attributeName) {
		return attributeName.endsWith("-ref");
	}

	static void setPropertyValue(Element element, BeanDefinitionBuilder builder, String attrName, String propertyName) {
		String attr = element.getAttribute(attrName);
		if (StringUtils.hasText(attr)) {
			builder.addPropertyValue(propertyName, attr);
		}
	}

	static void setPropertyValue(Element element, BeanDefinitionBuilder builder, String attrName) {
		setPropertyValue(element, builder, attrName, Conventions.attributeNameToPropertyName(attrName));
	}

	static boolean setPropertyReference(Element element, BeanDefinitionBuilder builder, String attrName,
			String propertyName) {
		String attr = element.getAttribute(attrName);
		if (StringUtils.hasText(attr)) {
			builder.addPropertyReference(propertyName, attr);
			return true;
		}
		return false;
	}

	static boolean setPropertyReference(Element element, BeanDefinitionBuilder builder, String attrName) {
		return setPropertyReference(element, builder, attrName,
				Conventions.attributeNameToPropertyName(isReference(attrName) ? attrName.substring(0,
						attrName.length() - 4) : attrName));
	}

	static boolean setCSVProperty(Element element, BeanDefinitionBuilder builder, String attrName, String propertyName) {
		String attr = element.getAttribute(attrName);
		if (StringUtils.hasText(attr)) {
			String[] strs = StringUtils.commaDelimitedListToStringArray(attr);
			ManagedList<String> list = new ManagedList<String>(strs.length);
			for (int i = 0; i < strs.length; i++) {
				list.add(strs[i].trim());
			}
			builder.addPropertyValue(propertyName, list);
			return true;
		}
		return false;
	}

	static boolean setCSVReferenceProperty(Element element, BeanDefinitionBuilder builder, String attrName,
			String propertyName) {
		String attr = element.getAttribute(attrName);
		if (StringUtils.hasText(attr)) {
			String[] strs = StringUtils.commaDelimitedListToStringArray(attr);
			ManagedList<RuntimeBeanReference> list = new ManagedList<RuntimeBeanReference>(strs.length);
			for (int i = 0; i < strs.length; i++) {
				list.add(new RuntimeBeanReference(strs[i].trim()));
			}
			builder.addPropertyValue(propertyName, list);
			return true;
		}
		return false;
	}

	static boolean setCSVProperty(Element element, BeanDefinitionBuilder builder, String attrName) {
		return setCSVProperty(element, builder, attrName, attrName);
	}

}
