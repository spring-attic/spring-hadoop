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
package org.springframework.yarn.integration.config;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.ConsumerEndpointFactoryBean;
import org.springframework.integration.ip.tcp.TcpOutboundGateway;
import org.springframework.integration.ip.tcp.connection.TcpNetClientConnectionFactory;
import org.springframework.util.StringUtils;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.config.YarnNamespaceUtils;
import org.springframework.yarn.integration.IntegrationAppmasterServiceClientFactoryBean;
import org.springframework.yarn.integration.ip.mind.MindRpcSerializer;
import org.w3c.dom.Element;

/**
 * Simple namespace parser for &lt;yarn-int:amservice-client&gt;.
 *
 * @author Janne Valkealahti
 *
 */
public class IntegrationAmServiceClientParser extends AbstractSingleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return IntegrationAppmasterServiceClientFactoryBean.class;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		// we create default spring int configuration if
		// both channels are not explicitly referenced.
		boolean hasResponseChannelAttribute = element.hasAttribute("response-channel");
		boolean hasRequestChannelAttribute = element.hasAttribute("request-channel");

		if(!hasResponseChannelAttribute && !hasRequestChannelAttribute) {

			BeanDefinitionBuilder defBuilder = BeanDefinitionBuilder.genericBeanDefinition(DirectChannel.class);
			AbstractBeanDefinition beanDef = defBuilder.getBeanDefinition();
			String reqChannelBeanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
			parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, reqChannelBeanName));
			builder.addPropertyReference("requestChannel", reqChannelBeanName);

			defBuilder = BeanDefinitionBuilder.genericBeanDefinition(QueueChannel.class);
			beanDef = defBuilder.getBeanDefinition();
			String repChannelBeanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
			parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, repChannelBeanName));
			builder.addPropertyReference("responseChannel", repChannelBeanName);

			defBuilder = BeanDefinitionBuilder.genericBeanDefinition(MindRpcSerializer.class);
			beanDef = defBuilder.getBeanDefinition();
			String serializerBeanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
			parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, serializerBeanName));

			defBuilder = BeanDefinitionBuilder.genericBeanDefinition(TcpNetClientConnectionFactory.class);

			String host = element.hasAttribute("host") ? element.getAttribute("host") : "localhost";
			String port = element.hasAttribute("port") ? element.getAttribute("port") : "0";

			defBuilder.addConstructorArgValue(host);
			defBuilder.addConstructorArgValue(port);
			defBuilder.addPropertyReference("serializer", serializerBeanName);
			defBuilder.addPropertyReference("deserializer", serializerBeanName);
			beanDef = defBuilder.getBeanDefinition();
			String conFactoryBeanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
			parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, conFactoryBeanName));

			defBuilder = BeanDefinitionBuilder.genericBeanDefinition(TcpOutboundGateway.class);
			defBuilder.addPropertyReference("connectionFactory", conFactoryBeanName);
			defBuilder.addPropertyReference("outputChannel", reqChannelBeanName);
			defBuilder.addPropertyReference("replyChannel", repChannelBeanName);
			beanDef = defBuilder.getBeanDefinition();
			String gatewayBeanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
			parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, gatewayBeanName));

			defBuilder = BeanDefinitionBuilder.genericBeanDefinition(ConsumerEndpointFactoryBean.class);
			defBuilder.addPropertyReference("handler", gatewayBeanName);
			defBuilder.addPropertyValue("inputChannelName", reqChannelBeanName);
			beanDef = defBuilder.getBeanDefinition();
			String consumerBeanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
			parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, consumerBeanName));

		} else if (hasResponseChannelAttribute && hasRequestChannelAttribute) {
			// both references given
			YarnNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "request-channel");
			YarnNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "response-channel");
		} else {
			parserContext.getReaderContext().error("Both request-channel and response-channel " +
					"needs to either exist together or omitted completely.", element);
		}

		YarnNamespaceUtils.setValueIfAttributeDefined(builder, element, "service-impl");
		YarnNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "object-mapper");

	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {
		String name = super.resolveId(element, definition, parserContext);
		if (!StringUtils.hasText(name)) {
			name = YarnSystemConstants.DEFAULT_ID_AMSERVICE_CLIENT;
		}
		return name;
	}

}
