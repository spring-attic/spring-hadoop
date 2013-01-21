/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.hadoop.cascading;

import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.cascade.CascadeDef;
import cascading.cascade.CascadeListener;
import cascading.cascade.CascadeProps;
import cascading.flow.Flow;
import cascading.flow.FlowSkipStrategy;

/**
 * Factory for declarative {@link Cascade} creation. The cascade is initialized but not started. 
 * 
 * @author Costin Leau
 */
public class CascadeFactoryBean implements InitializingBean, BeanNameAware, FactoryBean<Cascade> {

	private Configuration configuration;
	private Properties properties;
	private FlowSkipStrategy skipStrategy;
	private Collection<Flow> flows;
	private int concurrentFlows = -1;
	private String beanName;
	private Collection<CascadeListener> listeners;

	private Cascade cascade;
	private CascadeDef def;

	@Override
	public Cascade getObject() throws Exception {
		return cascade;
	}

	@Override
	public Class<?> getObjectType() {
		return (cascade != null ? cascade.getClass() : Cascade.class);
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Properties props = ConfigurationUtils.asProperties(ConfigurationUtils.createFrom(configuration, properties));

		CascadeDef cascadeDef = CascadeDef.cascadeDef();
		// copy definition

		if (def != null) {
			cascadeDef.addFlows(def.getFlows()).setName(def.getName()).setMaxConcurrentFlows(
					def.getMaxConcurrentFlows()).addTags(StringUtils.commaDelimitedListToStringArray(def.getTags()));
		}

		if (concurrentFlows != -1) {
			CascadeProps.setMaxConcurrentFlows(props, concurrentFlows);
			cascadeDef.setMaxConcurrentFlows(concurrentFlows);
		}

		cascadeDef.addFlows(flows);

		if (!StringUtils.hasText(cascadeDef.getName())) {
			cascadeDef.setName(beanName);
		}

		cascade = new CascadeConnector(properties).connect(cascadeDef);

		if (skipStrategy != null) {
			cascade.setFlowSkipStrategy(skipStrategy);
		}
		if (!CollectionUtils.isEmpty(listeners)) {
			for (CascadeListener listener : listeners) {
				cascade.addListener(listener);
			}
		}
	}

	/**
	 * Sets the configuration.
	 *
	 * @param configuration the new configuration
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Sets the properties.
	 *
	 * @param properties the new properties
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	/**
	 * Sets the flows.
	 *
	 * @param flows The flows to set.
	 */
	public void setFlows(Collection<Flow> flows) {
		this.flows = flows;
	}

	/**
	 * Sets the cascade definition.
	 * 
	 * @param def Cascade definition to use.
	 */
	public void setDefinition(CascadeDef def) {
		this.def = def;
	}

	/**
	 * Sets the listeners.
	 *
	 * @param listeners The listeners to add.
	 */
	public void setListeners(Collection<CascadeListener> listeners) {
		this.listeners = listeners;
	}
}