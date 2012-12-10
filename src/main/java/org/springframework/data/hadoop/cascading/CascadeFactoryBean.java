/*
 * Copyright 2011-2012 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;
import org.springframework.data.hadoop.util.ResourceUtils;
import org.springframework.util.Assert;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.cascade.CascadeProps;
import cascading.flow.Flow;
import cascading.flow.FlowSkipStrategy;
import cascading.property.AppProps;

/**
 * Factory for declarative {@link Cascade} creation. The cascade is initialized but not started. 
 * 
 * @author Costin Leau
 */
public class CascadeFactoryBean implements InitializingBean, BeanNameAware, FactoryBean<Cascade> {

	private static final Log log = LogFactory.getLog(CascadeFactoryBean.class);

	private Configuration configuration;
	private Properties properties;
	private FlowSkipStrategy skipStrategy;
	private Collection<Flow> flows;
	private int concurrentFlows = 0;
	private String beanName;

	private Class<?> jarClass;
	private Resource jar;

	private Cascade cascade;
	private boolean jarSetup = true;

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

		CascadeProps.setMaxConcurrentFlows(props, concurrentFlows);

		if (jarSetup) {
			if (jar != null) {
				AppProps.setApplicationJarPath(props, ResourceUtils.decode(jar.getURI().toString()));
			}
			else if (jarClass != null) {
				AppProps.setApplicationJarClass(props, jarClass);
			}
			else {
				// auto-detection based on the classpath
				ClassLoader cascadingCL = Cascade.class.getClassLoader();
				Resource cascadingCore = ResourceUtils.findContainingJar(Cascade.class);
				Resource cascadingHadoop = ResourceUtils.findContainingJar(cascadingCL, "cascading/flow/hadoop/HadoopFlow.class");
				// find jgrapht
				Resource jgrapht = ResourceUtils.findContainingJar(cascadingCL, "org/jgrapht/Graph.class");

				Assert.notNull(cascadingCore, "Cannot find cascading-core.jar");
				Assert.notNull(cascadingHadoop, "Cannot find cascading-hadoop.jar");
				Assert.notNull(jgrapht, "Cannot find jgraphts-jdk.jar");
				
				if (log.isDebugEnabled()) {
					log.debug("Auto-detecting Cascading Libs ["
							+ Arrays.toString(new Resource[] { cascadingCore, cascadingHadoop, jgrapht }) + "]");
				}

				ConfigurationUtils.addLibs(configuration, cascadingCore, cascadingHadoop, jgrapht);
			}
		}

		cascade = new CascadeConnector(properties).connect(beanName, flows);
		if (skipStrategy != null) {
			cascade.setFlowSkipStrategy(skipStrategy);
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
	 * Determines the job jar (available on the classpath) based on the given class.
	 * 
	 * @param jarClass The jarClass to set.
	 */
	public void setJarByClass(Class<?> jarClass) {
		this.jarClass = jarClass;
	}

	/**
	 * Sets the job jar (which might not be on the classpath).
	 * 
	 * @param jar The jar to set.
	 */
	public void setJar(Resource jar) {
		this.jar = jar;
	}

	/**
	 * Indicates whether the Cascading jar should be set for the cascade.
	 * By default it is true, meaning the factory will use the user provided settings
	 * ({@link #setJar(Resource)} and {@link #setJarByClass(Class)} or falling back
	 * to its own discovery mechanism if the above are not setup. 
	 * 
	 * When running against a cluster where cascading is already present, turn this to false
	 * to avoid shipping the library jar with the job.
	 * 
	 * @param jarSetup
	 */
	public void setJarSetup(boolean jarSetup) {
		this.jarSetup = jarSetup;
	}
}