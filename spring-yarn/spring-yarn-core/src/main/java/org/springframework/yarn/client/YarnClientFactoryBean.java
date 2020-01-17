/*
 * Copyright 2014-2016 the original author or authors.
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
package org.springframework.yarn.client;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.ClassUtils;
import org.springframework.yarn.fs.ResourceLocalizer;

/**
 * Factory bean building {@link YarnClient} instances.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnClientFactoryBean implements InitializingBean, FactoryBean<YarnClient> {

	/** Yarn configuration for client */
	private Configuration configuration;

	/** Template to set for client */
	private ClientRmOperations template;

	/** Client returned by this factory */
	private YarnClient client;

	private Class<? extends YarnClient> clientClass = CommandYarnClient.class;

	/** Container request priority */
	private int priority = 0;

	/** Resource capability as of cores */
	private int virtualcores = 1;

	/** Resource capability as of memory */
	private long memory = 64;

	/** Yarn queue for the request */
	private String queue = "default";

	/** Resource localizer for application master */
	private ResourceLocalizer resourceLocalizer;

	/** Environment for application master */
	private Map<String, String> environment;

	/** Commands starting application master */
	private List<String> commands;

	/** Name of the application */
	private String appName = "";

	/** Type of the application */
	private String appType;

	/** Application label expression */
	private String labelExpression;

	@Override
	public void afterPropertiesSet() throws Exception {
		// set template if not defined
		if(template == null) {
			ClientRmTemplate crmt = new ClientRmTemplate(configuration);
			crmt.afterPropertiesSet();
			template = crmt;
		}
		Constructor<? extends YarnClient> ctor = ClassUtils.getConstructorIfAvailable(clientClass,
				ClientRmOperations.class);
		client = BeanUtils.instantiateClass(ctor, template);
		if (client instanceof AbstractYarnClient) {
			AbstractYarnClient c = (AbstractYarnClient)client;
			c.setPriority(priority);
			c.setVirtualcores(virtualcores);
			c.setMemory(memory);
			c.setQueue(queue);
			c.setLabelExpression(labelExpression);
			c.setAppName(appName);
			c.setAppType(appType);
			c.setCommands(commands);
			c.setEnvironment(environment);
			c.setResourceLocalizer(resourceLocalizer);
			c.setConfiguration(configuration);
		}
	}

	@Override
	public YarnClient getObject() throws Exception {
		return client;
	}

	@Override
	public Class<YarnClient> getObjectType() {
		return YarnClient.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	/**
	 * Sets the client class.
	 *
	 * @param clientClass the new client class
	 */
	public void setClientClass(Class<? extends YarnClient> clientClass) {
		this.clientClass = clientClass;
	}

	/**
	 * Sets the Yarn configuration.
	 *
	 * @param configuration the Yarn configuration
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Sets the environment for appmaster.
	 *
	 * @param environment the environment
	 */
	public void setEnvironment(Map<String, String> environment) {
		this.environment = environment;
	}

	/**
	 * Sets the commands starting appmaster.
	 *
	 * @param commands the commands starting appmaster
	 */
	public void setCommands(List<String> commands) {
		this.commands = commands;
	}

	/**
	 * Sets the commands starting appmaster.
	 *
	 * @param commands the commands starting appmaster
	 */
	public void setCommands(String[] commands) {
		this.commands = Arrays.asList(commands);
	}

	/**
	 * Sets the resource localizer for appmaster container.
	 *
	 * @param resourceLocalizer the new resource localizer
	 */
	public void setResourceLocalizer(ResourceLocalizer resourceLocalizer) {
		this.resourceLocalizer = resourceLocalizer;
	}

	/**
	 * Sets the name for submitted application.
	 *
	 * @param appName the new application name
	 */
	public void setAppName(String appName) {
		this.appName = appName;
	}

	/**
	 * Sets the type for submitted application.
	 *
	 * @param appType the new application type
	 */
	public void setAppType(String appType) {
		this.appType = appType;
	}

	/**
	 * Sets the priority.
	 *
	 * @param priority the new priority
	 */
	public void setPriority(int priority) {
		this.priority = priority;
	}

	/**
	 * Sets the virtualcores.
	 *
	 * @param virtualcores the new virtualcores
	 */
	public void setVirtualcores(int virtualcores) {
		this.virtualcores = virtualcores;
	}

	/**
	 * Sets the memory.
	 *
	 * @param memory the new memory
	 */
	public void setMemory(long memory) {
		this.memory = memory;
	}

	/**
	 * Sets the queue.
	 *
	 * @param queue the new queue
	 */
	public void setQueue(String queue) {
		this.queue = queue;
	}

	/**
	 * Sets the application label expression.
	 *
	 * @param labelExpression the new application label expression
	 */
	public void setLabelExpression(String labelExpression) {
		this.labelExpression = labelExpression;
	}

	public void setTemplate(ClientRmOperations template) {
		this.template = template;
	}

}
