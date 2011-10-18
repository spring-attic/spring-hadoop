package org.springframework.data.hadoop.config;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.data.hadoop.configuration.ReducerFactoryBean;
import org.w3c.dom.Element;

/**
 * Simple parser for
 * {@link org.springframework.data.hadoop.configuration.ReducerFactoryBean} instances
 * 
 * @author Josh Long
 * @see org.springframework.data.hadoop.configuration.ReducerFactoryBean
 * @see org.springframework.data.hadoop.configuration.AutowiredJobFactoryBean
 * @since 1.0
 */
class ReducerParser extends AbstractSingleBeanDefinitionParser {

	/**
	 * the attribute for the 'target' bean to be used by the
	 * {@link org.springframework.data.hadoop.configuration.MapperFactoryBean factory
	 * bean}
	 */
	public static final String REF_ATTR = "ref";

	/**
	 * attribute specifying method that is optionally used (on the bean
	 * {@link MapperParser#REF_ATTR references}) in lieu of a method with an
	 * annotation
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

	/**
	 * attribute for the class of the value input into the reducer
	 */
	public static final String INPUT_VALUE_TYPE_ATTR = "input-value-type";

	@Override
	protected Class<?> getBeanClass(Element element) {
		return ReducerFactoryBean.class;
	}

	@Override
	protected void doParse(Element element, BeanDefinitionBuilder builder) {
		builder.addPropertyValue("inputValueType", element.getAttribute(INPUT_VALUE_TYPE_ATTR));
		builder.addPropertyValue("outputValueType", element.getAttribute(OUTPUT_VALUE_TYPE_ATTR));
		builder.addPropertyValue("outputKeyType", element.getAttribute(OUTPUT_KEY_TYPE_ATTR));
		builder.addPropertyReference("target", element.getAttribute(REF_ATTR));
		NamespaceUtils.setValueIfAttributeDefined(builder, element, METHOD_ATTR);
	}
}