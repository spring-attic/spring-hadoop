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
package org.springframework.yarn.configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * FactoryBean for creating a Map of environment variables.
 *
 * @author Janne Valkealahti
 *
 */
public class EnvironmentFactoryBean implements InitializingBean, FactoryBean<Map<String, String>> {

	/** Returned map will be build into this */
	private Map<String, String> environment;

	/** Incoming properties for environment */
	private Properties properties;

	/** Incoming classpath defined externally, i.e. nested properties. */
	private String classpath;

	/** Flag indicating if system env properties should be included */
	private boolean includeSystemEnv = true;

	/**
	 * Flag indicating if a default yarn entries should be added
	 * to a classpath. Effectively entries will be resolved from
	 * {@link YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH}.
	 */
	private boolean defaultYarnAppClasspath;

	/** Delimiter used in a classpath string */
	private String delimiter;

	@Override
	public Map<String, String> getObject() throws Exception {
		return environment;
	}

	@Override
	public Class<?> getObjectType() {
		return (environment != null ? environment.getClass() : Map.class);
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		environment = createEnvironment();

		if(properties != null) {
			// if we have properties, merge those into environment
			CollectionUtils.mergePropertiesIntoMap(properties, environment);
		}

		boolean addDelimiter = false;

		// set CLASSPATH variable if there's something to set
		StringBuilder classPathEnv = new StringBuilder();
		if(StringUtils.hasText(classpath)) {
			classPathEnv.append(classpath);
			addDelimiter = true;
		}

		if(defaultYarnAppClasspath) {
			ArrayList<String> paths = new ArrayList<String>();
			paths.add("$" + ApplicationConstants.Environment.HADOOP_CONF_DIR);
			paths.add("$" + ApplicationConstants.Environment.HADOOP_COMMON_HOME + "/*");
			paths.add("$" + ApplicationConstants.Environment.HADOOP_COMMON_HOME + "/lib/*");
			paths.add("$" + ApplicationConstants.Environment.HADOOP_COMMON_HOME + "/share/hadoop/common/*");
			paths.add("$" + ApplicationConstants.Environment.HADOOP_COMMON_HOME + "/share/hadoop/common/lib/*");
			paths.add("$" + ApplicationConstants.Environment.HADOOP_HDFS_HOME + "/*");
			paths.add("$" + ApplicationConstants.Environment.HADOOP_HDFS_HOME + "/lib*");
			paths.add("$" + ApplicationConstants.Environment.HADOOP_HDFS_HOME + "/share/hadoop/hdfs/*");
			paths.add("$" + ApplicationConstants.Environment.HADOOP_HDFS_HOME + "/share/hadoop/hdfs/lib/*");
			paths.add("$YARN_HOME/*");
			paths.add("$YARN_HOME/lib*");
			paths.add("$HADOOP_YARN_HOME/share/hadoop/yarn/*");
			paths.add("$HADOOP_YARN_HOME/share/hadoop/yarn/lib*");

			Iterator<String> iterator = paths.iterator();

			// add delimiter if we're about to add something
			if(iterator.hasNext()) {
				classPathEnv.append(addDelimiter ? delimiter : "");
			}

			while(iterator.hasNext()) {
				classPathEnv.append(iterator.next());
				if(iterator.hasNext()) {
					classPathEnv.append(delimiter);
				}
			}
		}

		String classpathString = classPathEnv.toString();
		if(StringUtils.hasText(classpathString)) {
			environment.put("CLASSPATH", classpathString);
		}
	}

	/**
	 * If set to true properties from a {@link System#getenv()} will
	 * be included to environment settings. Default value is true.
	 *
	 * @param includeSystemEnv flag to set
	 */
	public void setIncludeSystemEnv(boolean includeSystemEnv) {
		this.includeSystemEnv = includeSystemEnv;
	}

	/**
	 * Creates the {@link Map} to be returned from this factory bean.
	 *
	 * @return map of environment variables
	 */
	protected Map<String, String> createEnvironment() {
		if(includeSystemEnv) {
			return new HashMap<String, String>(System.getenv());
		} else {
			return new HashMap<String, String>();
		}
	}

	/**
	 * Sets the configuration properties.
	 *
	 * @param properties The properties to set.
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	/**
	 * Sets incoming classpath.
	 *
	 * @param classpath the incoming classpath to set
	 */
	public void setClasspath(String classpath) {
		this.classpath = classpath;
	}

	/**
	 * If set to true a default 'yarn' entries will be added to
	 * a 'CLASSPATH' environment variable.
	 *
	 * @param defaultYarnAppClasspath Flag telling if default yarn entries
	 *                                should be added to classpath
	 */
	public void setDefaultYarnAppClasspath(boolean defaultYarnAppClasspath) {
		this.defaultYarnAppClasspath = defaultYarnAppClasspath;
	}

	/**
	 * Sets the delimiter used in a classpath.
	 *
	 * @param delimiter delimiter to use in classpath
	 */
	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

}
