/*
 * Copyright 2013-2014 the original author or authors.
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
package org.springframework.yarn.batch.config;

import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.StepLocator;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.ConsumerEndpointFactoryBean;
import org.springframework.integration.config.IntegrationConverter;
import org.springframework.integration.ip.tcp.TcpOutboundGateway;
import org.springframework.integration.ip.tcp.connection.TcpNetClientConnectionFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.batch.repository.RemoteJobExplorer;
import org.springframework.yarn.batch.repository.RemoteJobRepository;
import org.springframework.yarn.batch.support.BeanFactoryStepLocator;
import org.springframework.yarn.integration.convert.MindHolderToObjectConverter;
import org.springframework.yarn.integration.convert.MindObjectToHolderConverter;
import org.springframework.yarn.integration.ip.mind.AppmasterMindScOperations;
import org.springframework.yarn.integration.ip.mind.DefaultMindAppmasterServiceClient;
import org.springframework.yarn.integration.ip.mind.MindRpcSerializer;
import org.springframework.yarn.integration.support.Jackson2ObjectMapperFactoryBean;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Annotation based batch configuration for Yarn Container.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
public class SimpleYarnRemoteBatchConfiguration {

	@Value("${SHDP_AMSERVICE_HOST}")
	private String host;

	@Value("${SHDP_AMSERVICE_PORT}")
	private int port;

	@Bean
	public JobBuilderFactory jobBuilders() throws Exception {
		return new JobBuilderFactory(jobRepository());
	}

	@Bean
	public StepBuilderFactory stepBuilders() throws Exception {
		return new StepBuilderFactory(jobRepository(), transactionManager());
	}

	@Bean
	public PlatformTransactionManager transactionManager() throws Exception {
		return new ResourcelessTransactionManager();
	}

	@Bean
	public StepLocator stepLocator() {
		return new BeanFactoryStepLocator();
	}

	@Bean
	public JobRepository jobRepository() {
		return new RemoteJobRepository(appmasterServiceClient());
	}

	@Bean
	public JobExplorer jobExplorer() {
		return new RemoteJobExplorer(appmasterServiceClient());
	}

	@Bean
	public DirectChannel directChannel() {
		return new DirectChannel();
	}

	@Bean
	public QueueChannel queueChannel() {
		return new QueueChannel();
	}

	@Bean
	public MindRpcSerializer mindRpcSerializer() {
		return new MindRpcSerializer();
	}

	@Bean
	public TcpNetClientConnectionFactory tcpNetClientConnectionFactory() {
		TcpNetClientConnectionFactory factory = new TcpNetClientConnectionFactory(host, port);
		factory.setSerializer(mindRpcSerializer());
		factory.setDeserializer(mindRpcSerializer());
		return factory;
	}

	@Bean
	public TcpOutboundGateway tcpOutboundGateway() {
		TcpOutboundGateway gateway = new TcpOutboundGateway();
		gateway.setConnectionFactory(tcpNetClientConnectionFactory());
		gateway.setOutputChannel(directChannel());
		gateway.setReplyChannel(queueChannel());
		gateway.setRequestTimeout(60000);
		gateway.setRemoteTimeout(60000);
		return gateway;
	}

	@Bean
	public ConsumerEndpointFactoryBean consumerEndpointFactoryBean() {
		ConsumerEndpointFactoryBean endpointFactoryBean = new ConsumerEndpointFactoryBean();
		endpointFactoryBean.setHandler(tcpOutboundGateway());
		endpointFactoryBean.setInputChannel(directChannel());
		return endpointFactoryBean;
	}

	@Bean(name=YarnSystemConstants.DEFAULT_ID_AMSERVICE_CLIENT)
	public AppmasterMindScOperations appmasterServiceClient() {
		DefaultMindAppmasterServiceClient client = new DefaultMindAppmasterServiceClient();
		client.setRequestChannel(directChannel());
		client.setResponseChannel(queueChannel());
		return client;
	}

	@Bean
	public ObjectMapper objectMapper() {
		Jackson2ObjectMapperFactoryBean factory = new Jackson2ObjectMapperFactoryBean();
		factory.afterPropertiesSet();
		return factory.getObject();
	}

	@Bean
	@IntegrationConverter
	public MindObjectToHolderConverter mindObjectToHolderConverter() {
		return new MindObjectToHolderConverter(objectMapper());
	}

	@Bean
	@IntegrationConverter
	public MindHolderToObjectConverter mindHolderToObjectConverter() {
		String[] packages = new String[]{
				"org.springframework.yarn.batch.repository.bindings",
				"org.springframework.yarn.batch.repository.bindings.exp",
				"org.springframework.yarn.batch.repository.bindings.repo"};
		return new MindHolderToObjectConverter(objectMapper(), packages);
	}

}
