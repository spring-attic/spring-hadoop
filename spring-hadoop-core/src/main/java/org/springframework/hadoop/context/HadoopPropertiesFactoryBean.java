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

package org.springframework.hadoop.context;

import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * A factory for Properties enumerating the default Hadoop configuration
 * settings for the current environment. The usual default Hadoop configuration
 * resources are consulted (i.e. <code>classpath:conf/core-site.xml</code> etc.)
 * and converted to a Properties instance for easy reference inside a Spring
 * application context.
 * 
 * @author Dave Syer
 * 
 */
public class HadoopPropertiesFactoryBean implements FactoryBean<Properties>, InitializingBean {

	private Properties instance = new Properties();

	private Configuration configuration = new Configuration();
	
	/**
	 * @param configuration the configuration to set
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public Properties getObject() throws Exception {
		return instance;
	}

	public Class<?> getObjectType() {
		return Properties.class;
	}

	public boolean isSingleton() {
		return true;
	}

	public void afterPropertiesSet() throws Exception {
		configuration.get("dummy"); // force load resources
		for (Entry<String, String> entry : configuration) {
			instance.setProperty(entry.getKey(), entry.getValue());
		}
	}

}
