/*
 * Copyright 2006-2010 the original author or authors.
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

package org.springframework.data.hadoop.context;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

/**
 * @author Dave Syer
 * 
 */
public class HadoopConfigurationFactoryBean implements FactoryBean<Configuration>, BeanFactoryPostProcessor {

	private Configuration instance = new Configuration();

	public Configuration getObject() throws Exception {
		return instance;
	}

	public Class<?> getObjectType() {
		return Properties.class;
	}

	public boolean isSingleton() {
		return true;
	}

	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
		if (beanFactory.containsSingleton(DefaultContextLoader.SPRING_CONFIG_EXTRA)) {
			Configuration bean = beanFactory.getBean(DefaultContextLoader.SPRING_CONFIG_EXTRA, Configuration.class);
			instance = bean;
		}
		instance.setBoolean(DefaultContextLoader.SPRING_CONFIG_EXTRA, true);
	}

}
