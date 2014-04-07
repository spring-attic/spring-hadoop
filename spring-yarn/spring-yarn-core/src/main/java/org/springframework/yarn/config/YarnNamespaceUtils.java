/*
 * Copyright 2014 the original author or authors.
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

import java.util.List;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.BeanDefinitionParserDelegate;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.core.Conventions;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * Shared utility methods for yarn namespace parsers.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnNamespaceUtils {

	static final String REF_ATTRIBUTE = "ref";

	/**
	 * Configures the provided bean definition builder with a property value
	 * corresponding to the attribute whose name is provided if that attribute
	 * is defined in the given element.
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be used to populate the property
	 * @param propertyName the name of the property to be populated
	 */
	public static void setValueIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName, String propertyName) {
		setValueIfAttributeDefined(builder, element, attributeName, propertyName, false);
	}

	/**
	 * Configures the provided bean definition builder with a property value
	 * corresponding to the attribute whose name is provided if that attribute
	 * is defined in the given element.
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be used to populate the property
	 * @param propertyName the name of the property to be populated
	 * @param defaultPropertyValue Property value to use as default
	 */
	public static void setValueIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName, String propertyName, String defaultPropertyValue) {
		setValueIfAttributeDefined(builder, element, attributeName, propertyName, false, defaultPropertyValue);
	}

	/**
	 * Configures the provided bean definition builder with a property value
	 * corresponding to the attribute whose name is provided if that attribute
	 * is defined in the given element.
	 * <p>
	 * The property name will be the camel-case equivalent of the lower case
	 * hyphen separated attribute (e.g. the "foo-bar" attribute would match the
	 * "fooBar" property).
	 *
	 * @see Conventions#attributeNameToPropertyName(String)
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be set on the property
	 */
	public static void setValueIfAttributeDefined(BeanDefinitionBuilder builder, Element element, String attributeName) {
		setValueIfAttributeDefined(builder, element, attributeName, false);
	}

	/**
	 * Configures the provided bean definition builder with a property value
	 * corresponding to the attribute whose name is provided if that attribute
	 * is defined in the given element.
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be
	 *            used to populate the property
	 * @param propertyName the name of the property to be populated
	 * @param emptyStringAllowed if true, the value is set, even if an empty String (""); if
	 *            false, an empty String is treated as if the attribute wasn't provided.
	 */
	public static void setValueIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName, String propertyName, boolean emptyStringAllowed) {
		String attributeValue = element.getAttribute(attributeName);
		if (StringUtils.hasText(attributeValue) || (emptyStringAllowed && element.hasAttribute(attributeName))) {
			builder.addPropertyValue(propertyName, new TypedStringValue(attributeValue));
		}
	}

	/**
	 * Configures the provided bean definition builder with a property value
	 * corresponding to the attribute whose name is provided if that attribute
	 * is defined in the given element.
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be
	 *            used to populate the property
	 * @param propertyName the name of the property to be populated
	 * @param emptyStringAllowed if true, the value is set, even if an empty String (""); if
	 *            false, an empty String is treated as if the attribute wasn't provided.
	 * @param defaultPropertyValue Property value to use as default
	 */
	public static void setValueIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName, String propertyName, boolean emptyStringAllowed, String defaultPropertyValue) {
		String attributeValue = element.getAttribute(attributeName);
		if (StringUtils.hasText(attributeValue) || (emptyStringAllowed && element.hasAttribute(attributeName))) {
			builder.addPropertyValue(propertyName, new TypedStringValue(attributeValue));
		} else if (StringUtils.hasText(defaultPropertyValue)) {
			builder.addPropertyValue(propertyName, new TypedStringValue(defaultPropertyValue));
		}
	}

	/**
	 * Configures the provided bean definition builder with a property value
	 * corresponding to the attribute whose name is provided if that attribute
	 * is defined in the given element.
	 * <p>
	 * The property name will be the camel-case equivalent of the lower case
	 * hyphen separated attribute (e.g. the "foo-bar" attribute would match the
	 * "fooBar" property).
	 *
	 * @see Conventions#attributeNameToPropertyName(String)
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be set on the property
	 * @param emptyStringAllowed if true, the value is set, even if an empty String (""); if
	 *            false, an empty String is treated as if the attribute wasn't provided.
	 */
	public static void setValueIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName, boolean emptyStringAllowed) {
		setValueIfAttributeDefined(builder, element, attributeName,
				Conventions.attributeNameToPropertyName(attributeName), emptyStringAllowed);
	}

	/**
	 * Configures the provided bean definition builder with a property value
	 * corresponding to the attribute whose name is provided if that attribute
	 * is defined in the given element.
	 * <p>
	 * The property name will be the camel-case equivalent of the lower case
	 * hyphen separated attribute (e.g. the "foo-bar" attribute would match the
	 * "fooBar" property).
	 *
	 * @see Conventions#attributeNameToPropertyName(String)
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be set on the property
	 * @param emptyStringAllowed if true, the value is set, even if an empty String (""); if
	 *            false, an empty String is treated as if the attribute wasn't provided.
	 * @param defaultPropertyValue Property value to use as default
	 */
	public static void setValueIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName, boolean emptyStringAllowed, String defaultPropertyValue) {
		setValueIfAttributeDefined(builder, element, attributeName,
				Conventions.attributeNameToPropertyName(attributeName), emptyStringAllowed, defaultPropertyValue);
	}

	/**
	 * Configures the provided bean definition builder with a property reference
	 * to a bean. The bean reference is identified by the value from the
	 * attribute whose name is provided if that attribute is defined in the
	 * given element.
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be used as a bean
	 *            reference to populate the property
	 * @param defaultName the default bean reference name to use
	 * @param propertyName the name of the property to be populated
	 */
	public static void setReferenceIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName, String propertyName, String defaultName) {
		String attributeValue = element.getAttribute(attributeName);
		if (StringUtils.hasText(attributeValue)) {
			builder.addPropertyReference(propertyName, attributeValue);
		} else if (StringUtils.hasText(defaultName)) {
			builder.addPropertyReference(propertyName, defaultName);
		}
	}

	/**
	 * Configures the provided bean definition builder with a property reference
	 * to a bean. The bean reference is identified by the value from the
	 * attribute whose name is provided if that attribute is defined in the
	 * given element.
	 * <p>
	 * The property name will be the camel-case equivalent of the lower case
	 * hyphen separated attribute (e.g. the "foo-bar" attribute would match the
	 * "fooBar" property).
	 *
	 * @see Conventions#attributeNameToPropertyName(String)
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be used as a bean
	 *            reference to populate the property
	 *
	 * @see Conventions#attributeNameToPropertyName(String)
	 */
	public static void setReferenceIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName) {
		setReferenceIfAttributeDefined(builder, element, attributeName,
				Conventions.attributeNameToPropertyName(attributeName), null);
	}

	/**
	 * Configures the provided bean definition builder with a property reference
	 * to a bean. The bean reference is identified by the value from the
	 * attribute whose name is provided if that attribute is defined in the
	 * given element.
	 * <p>
	 * The property name will be the camel-case equivalent of the lower case
	 * hyphen separated attribute (e.g. the "foo-bar" attribute would match the
	 * "fooBar" property).
	 *
	 * @see Conventions#attributeNameToPropertyName(String)
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be used as a bean
	 *            reference to populate the property
	 * @param defaultName the default bean reference name to use
	 *
	 * @see Conventions#attributeNameToPropertyName(String)
	 */
	public static void setReferenceIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName, String defaultName) {
		setReferenceIfAttributeDefined(builder, element, attributeName,
				Conventions.attributeNameToPropertyName(attributeName), defaultName);
	}

	/**
	 * Parses inner bean definition from an dom element.
	 *
	 * @param element the dom element
	 * @param parserContext the parser context
	 * @return bean component definition
	 */
	public static BeanComponentDefinition parseInnerHandlerDefinition(Element element, ParserContext parserContext) {
		// parses out the inner bean definition for concrete implementation if defined
		List<Element> childElements = DomUtils.getChildElementsByTagName(element, "bean");
		BeanComponentDefinition innerComponentDefinition = null;
		if (childElements != null && childElements.size() == 1) {
			Element beanElement = childElements.get(0);
			BeanDefinitionParserDelegate delegate = parserContext.getDelegate();
			BeanDefinitionHolder bdHolder = delegate.parseBeanDefinitionElement(beanElement);
			bdHolder = delegate.decorateBeanDefinitionIfRequired(beanElement, bdHolder);
			BeanDefinition inDef = bdHolder.getBeanDefinition();
			innerComponentDefinition = new BeanComponentDefinition(inDef, bdHolder.getBeanName());
		}
		String ref = element.getAttribute(REF_ATTRIBUTE);
		if (StringUtils.hasText(ref) && innerComponentDefinition != null) {
			parserContext.getReaderContext().error(
					"Ambiguous definition. Inner bean "
							+ (innerComponentDefinition == null ? innerComponentDefinition : innerComponentDefinition
									.getBeanDefinition().getBeanClassName()) + " declaration and \"ref\" " + ref
							+ " are not allowed together on element " + createElementDescription(element) + ".",
					parserContext.extractSource(element));
		}
		return innerComponentDefinition;
	}

	/**
	 * Provides a user friendly description of an element based on its node name
	 * and, if available, its "id" attribute value. This is useful for creating
	 * error messages from within bean definition parsers.
	 *
	 * @param element the dom element
	 * @return user friendly element description
	 */
	public static String createElementDescription(Element element) {
		String elementId = "'" + element.getNodeName() + "'";
		String id = element.getAttribute("id");
		if (StringUtils.hasText(id)) {
			elementId += " with id='" + id + "'";
		}
		return elementId;
	}

	/**
	 * Sets the reference property as list from a comma delimited string.
	 *
	 * @param element the element
	 * @param builder the builder
	 * @param attrName the attr name
	 * @param propertyName the property name
	 * @return true, if successful
	 */
	public static boolean setCSVReferenceProperty(Element element, BeanDefinitionBuilder builder, String attrName,
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

}
