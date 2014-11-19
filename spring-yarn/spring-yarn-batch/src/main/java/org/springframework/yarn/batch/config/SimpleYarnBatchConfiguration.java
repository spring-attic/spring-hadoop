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

import javax.sql.DataSource;

import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.IntegrationConverter;
import org.springframework.integration.ip.tcp.TcpInboundGateway;
import org.springframework.integration.ip.tcp.connection.TcpNetServerConnectionFactory;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.AppmasterService;
import org.springframework.yarn.batch.repository.BatchAppmasterService;
import org.springframework.yarn.batch.repository.JobRepositoryService;
import org.springframework.yarn.event.DefaultYarnEventPublisher;
import org.springframework.yarn.event.YarnEventPublisher;
import org.springframework.yarn.integration.convert.MindHolderToObjectConverter;
import org.springframework.yarn.integration.convert.MindObjectToHolderConverter;
import org.springframework.yarn.integration.ip.mind.MindRpcSerializer;
import org.springframework.yarn.integration.support.DefaultPortExposingTcpSocketSupport;
import org.springframework.yarn.integration.support.Jackson2ObjectMapperFactoryBean;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Annotation based batch configuration for Yarn Appmaster.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
public class SimpleYarnBatchConfiguration {

	@Autowired
	private DataSource dataSource;

	@Autowired
	private JobRepository jobRepository;

	@Bean
	public YarnEventPublisher yarnEventPublisher() {
		return new DefaultYarnEventPublisher();
	}

	@Bean
	public JobRepositoryService jobRepositoryService() throws Exception {
		JobRepositoryService service = new JobRepositoryService();
		service.setJobExplorer(jobExplorer());
		service.setJobRepository(jobRepository);
		return service;
	}

	@Bean
	public JobExplorer jobExplorer() throws Exception {
		JobExplorerFactoryBean factory = new JobExplorerFactoryBean();
		factory.setDataSource(dataSource);
		factory.afterPropertiesSet();
		return (JobExplorer) factory.getObject();
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

	@Bean
	public DirectChannel directChannel() {
		return new DirectChannel();
	}

	@Bean
	public MindRpcSerializer mindRpcSerializer() {
		return new MindRpcSerializer();
	}

	@Bean
	public DefaultPortExposingTcpSocketSupport defaultPortExposingTcpSocketSupport() {
		return new DefaultPortExposingTcpSocketSupport();
	}

	@Bean
	public TcpNetServerConnectionFactory tcpNetServerConnectionFactory() {
		TcpNetServerConnectionFactory factory = new TcpNetServerConnectionFactory(0);
		factory.setTcpSocketSupport(defaultPortExposingTcpSocketSupport());
		factory.setSerializer(mindRpcSerializer());
		factory.setDeserializer(mindRpcSerializer());
		return factory;
	}

	@Bean
	public TcpInboundGateway tcpInboundGateway() {
		TcpInboundGateway gateway = new TcpInboundGateway();
		gateway.setConnectionFactory(tcpNetServerConnectionFactory());
		gateway.setRequestChannel(directChannel());
		return gateway;
	}

	@Bean(name=YarnSystemConstants.DEFAULT_ID_AMSERVICE)
	public AppmasterService appmasterService() throws Exception {
		BatchAppmasterService service = new BatchAppmasterService();
		service.setJobRepositoryRemoteService(jobRepositoryService());
		service.setMessageChannel(directChannel());
		service.setSocketSupport(defaultPortExposingTcpSocketSupport());
		return service;
	}

}
