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
import org.springframework.integration.ip.tcp.TcpInboundGateway;
import org.springframework.integration.ip.tcp.connection.TcpNetServerConnectionFactory;
import org.springframework.util.StringUtils;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.config.YarnNamespaceUtils;
import org.springframework.yarn.integration.IntegrationAppmasterServiceFactoryBean;
import org.springframework.yarn.integration.ip.mind.MindRpcSerializer;
import org.springframework.yarn.integration.support.DefaultPortExposingTcpSocketSupport;
import org.w3c.dom.Element;

/**
 * Simple namespace parser for &lt;yarn-int:amservice&gt;.
 *
 * @author Janne Valkealahti
 *
 */
public class IntegrationAmServiceParser extends AbstractSingleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return IntegrationAppmasterServiceFactoryBean.class;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		boolean hasChannelAttribute = element.hasAttribute("channel");
		if(!hasChannelAttribute) {

			// service dispatch channel
			BeanDefinitionBuilder defBuilder = BeanDefinitionBuilder.genericBeanDefinition(DirectChannel.class);
			AbstractBeanDefinition beanDef = defBuilder.getBeanDefinition();
			String channelBeanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
			parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, channelBeanName));
			builder.addPropertyReference("channel", channelBeanName);

			// serializer
			defBuilder = BeanDefinitionBuilder.genericBeanDefinition(MindRpcSerializer.class);
			beanDef = defBuilder.getBeanDefinition();
			String serializerBeanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
			parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, serializerBeanName));

			// socket support
			defBuilder = BeanDefinitionBuilder.genericBeanDefinition(DefaultPortExposingTcpSocketSupport.class);
			beanDef = defBuilder.getBeanDefinition();
			String socketSupportBeanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
			parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, socketSupportBeanName));
			builder.addPropertyReference("socketSupport", socketSupportBeanName);

			// connection factory
			defBuilder = BeanDefinitionBuilder.genericBeanDefinition(TcpNetServerConnectionFactory.class);
			String port = element.hasAttribute("default-port") ? element.getAttribute("default-port") : "0";
			defBuilder.addConstructorArgValue(port);
			defBuilder.addPropertyReference("tcpSocketSupport", socketSupportBeanName);
			defBuilder.addPropertyReference("serializer", serializerBeanName);
			defBuilder.addPropertyReference("deserializer", serializerBeanName);
			beanDef = defBuilder.getBeanDefinition();
			String connectionFactoryBeanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
			parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, connectionFactoryBeanName));

			// gateway
			defBuilder = BeanDefinitionBuilder.genericBeanDefinition(TcpInboundGateway.class);
			defBuilder.addPropertyReference("connectionFactory", connectionFactoryBeanName);
			defBuilder.addPropertyReference("requestChannel", channelBeanName);
			beanDef = defBuilder.getBeanDefinition();
			String gatewayBeanName = BeanDefinitionReaderUtils.generateBeanName(beanDef, parserContext.getRegistry());
			parserContext.registerBeanComponent(new BeanComponentDefinition(beanDef, gatewayBeanName));

			builder.addPropertyReference("channel", channelBeanName);

		} else {
			YarnNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "channel");
		}

		YarnNamespaceUtils.setValueIfAttributeDefined(builder, element, "service-impl");
		YarnNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "service-ref");
		YarnNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "object-mapper");
		YarnNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "socket-support");

	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {
		String name = super.resolveId(element, definition, parserContext);
		if (!StringUtils.hasText(name)) {
			name = YarnSystemConstants.DEFAULT_ID_AMSERVICE;
		}
		return name;
	}

}
