/*
 * Copyright 2014 the original author or authors.
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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * FactoryBean for creating a Map of environment variables.
 *
 * @author Janne Valkealahti
 *
 */
public class EnvironmentFactoryBean implements InitializingBean, FactoryBean<Map<String, String>> {

	private static final Log log = LogFactory.getLog(EnvironmentFactoryBean.class);

	/** Returned map will be build into this */
	private Map<String, String> environment;

	/** Incoming properties for environment */
	private Properties properties;

	/** Incoming classpath defined externally, i.e. nested properties. */
	private String classpath;

	/** Incoming default yarn classpath. */
	private String defaultYarnAppClasspath;

	/** Incoming default mr classpath. */
	private String defaultMapreduceAppClasspath;

	/** Flag indicating if system env properties should be included */
	private boolean includeLocalSystemEnv = false;

	/**
	 * Flag indicating if a default yarn entries should be added
	 * to a classpath. Effectively entries will be resolved from
	 * {@link YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH}.
	 */
	private boolean useDefaultYarnClasspath = false;

	/**
	 * Flag indicating if a default rm entries should be added
	 * to a classpath.
	 */
	private boolean useDefaultMapreduceClasspath = false;

	/**
	 * Flag indicating if base directory should included
	 * when building classpath. Entry in a classpath will
	 * simply be "./*".
	 */
	private boolean includeBaseDirectory;

	/** Delimiter used in a classpath string */
	private String delimiter;

	/** Yarn configuration */
	private Configuration configuration;

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

		ArrayList<String> paths = new ArrayList<String>();

		if (includeBaseDirectory) {
			paths.add("./*");
		}

		if (useDefaultYarnClasspath) {
			if (log.isDebugEnabled()) {
				log.debug("Trying to use a default yarn classpath");
			}

			String defaultYarnClasspathString = "";
			if (StringUtils.hasText(defaultYarnAppClasspath)) {
				defaultYarnClasspathString = defaultYarnAppClasspath;
			} else if (configuration != null) {
				defaultYarnClasspathString = configuration.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH);
				if (!StringUtils.hasText(defaultYarnClasspathString)) {
					// 2.3.x changed yarn.application.classpath to be empty in yarn-default.xml, so
					// if we got empty, fall back to DEFAULT_YARN_APPLICATION_CLASSPATH
					defaultYarnClasspathString = StringUtils
							.arrayToCommaDelimitedString(YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
					log.info("Yarn classpath from configuration empty, fall back to " + defaultYarnClasspathString);
				} else {
					log.info("Yarn classpath from configuration " + defaultYarnClasspathString);
				}
			}
			paths.addAll(StringUtils.commaDelimitedListToSet(defaultYarnClasspathString));
		}

		if (useDefaultMapreduceClasspath) {
			if (log.isDebugEnabled()) {
				log.debug("Trying to use a mr yarn classpath");
			}
			String defaultMapreduceClasspathString = "";
			if (StringUtils.hasText(defaultMapreduceAppClasspath)) {
				defaultMapreduceClasspathString = defaultMapreduceAppClasspath;
			} else if (configuration != null) {
				defaultMapreduceClasspathString = configuration.get("mapreduce.application.classpath");
				if (!StringUtils.hasText(defaultMapreduceClasspathString)) {
					// using reflection with this because we don't have these
					// classes in a project classpath and this is just
					// a fall back for default value
					defaultMapreduceClasspathString = readStaticField("org.apache.hadoop.mapreduce.MRJobConfig",
							"DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH", getClass().getClassLoader());
					log.info("Mapreduce classpath from configuration empty, fall back to " + defaultMapreduceClasspathString);
				} else {
					log.info("Mapreduce classpath from configuration " + defaultMapreduceClasspathString);
				}
			}
			paths.addAll(StringUtils.commaDelimitedListToSet(defaultMapreduceClasspathString));
		}

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

		String classpathString = classPathEnv.toString();
		if(StringUtils.hasText(classpathString)) {
			environment.put("CLASSPATH", classpathString);
			log.info("Adding CLASSPATH=" + classpathString);
		}
	}

	/**
	 * If set to true properties from a {@link System#getenv()} will
	 * be included to environment settings. Default value is true.
	 *
	 * @param includeLocalSystemEnv flag to set
	 */
	public void setIncludeLocalSystemEnv(boolean includeLocalSystemEnv) {
		this.includeLocalSystemEnv = includeLocalSystemEnv;
	}

	/**
	 * Sets the yarn configuration.
	 *
	 * @param configuration the new yarn configuration
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Creates the {@link Map} to be returned from this factory bean.
	 *
	 * @return map of environment variables
	 */
	protected Map<String, String> createEnvironment() {
		if(includeLocalSystemEnv) {
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
	 * Sets the default yarn app classpath.
	 *
	 * @param defaultYarnAppClasspath the new default yarn app classpath
	 */
	public void setDefaultYarnAppClasspath(String defaultYarnAppClasspath) {
		this.defaultYarnAppClasspath = defaultYarnAppClasspath;
	}

	/**
	 * Sets the default mr app classpath.
	 *
	 * @param defaultMapreduceAppClasspath the new default mr app classpath
	 */
	public void setDefaultMapreduceAppClasspath(String defaultMapreduceAppClasspath) {
		this.defaultMapreduceAppClasspath = defaultMapreduceAppClasspath;
	}

	/**
	 * If set to true a default 'yarn' entries will be added to
	 * a 'CLASSPATH' environment variable.
	 *
	 * @param useDefaultYarnClasspath Flag telling if default yarn entries
	 *                                should be added to classpath
	 */
	public void setUseDefaultYarnClasspath(boolean useDefaultYarnClasspath) {
		this.useDefaultYarnClasspath = useDefaultYarnClasspath;
	}

	/**
	 * If set to true a default 'mr' entries will be added to
	 * a 'CLASSPATH' environment variable.
	 *
	 * @param useDefaultMapreduceClasspath Flag telling if default mr entries
	 *                                should be added to classpath
	 */
	public void setUseDefaultMapreduceClasspath(boolean useDefaultMapreduceClasspath) {
		this.useDefaultMapreduceClasspath = useDefaultMapreduceClasspath;
	}

	/**
	 * If set to true a base directory entry will be added to
	 * a 'CLASSPATH' environment variable.
	 *
	 * @param includeBaseDirectory Flag telling if base directory entry
	 *                             should be added to classpath
	 */
	public void setIncludeBaseDirectory(boolean includeBaseDirectory) {
		this.includeBaseDirectory = includeBaseDirectory;
	}

	/**
	 * Sets the delimiter used in a classpath.
	 *
	 * @param delimiter delimiter to use in classpath
	 */
	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	/**
	 * Utility method reading static String values from a class.
	 * @param clazzName full class name
	 * @param fieldName field name
	 * @param classLoader classloader
	 * @return the value or empty in any other case
	 */
	private static String readStaticField(String clazzName, String fieldName, ClassLoader classLoader) {
		try {
			Class<?> clazz = ClassUtils.forName(clazzName, classLoader);
			Field field = clazz.getField(fieldName);
			return (String) field.get(null);
		} catch (Error e) {
			log.warn("Unable to read static " + fieldName + " from " + clazzName, e);
		} catch (Exception e) {
			log.warn("Unable to read static " + fieldName + " from " + clazzName, e);
		}
		return "";
	}

}
