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
package org.springframework.data.hadoop.test.support.compat;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.BeanUtils;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

/**
 * Compatibility class accessing minimr classes at runtime
 * without knowing implementations during the compile time.
 * <p>
 * Usually minimr clusters are constructed via MiniMRClientClusterFactory
 * which have same methods for hadoop 1.x and 2.x. Hadoop have this
 * to easy testing for both versions. However at least hadoop 1.x distribution
 * from a Cloudera(chd3, chd4) don't have this factory method for we need
 * to resolve underlying cluster classes at runtime. Effectively these legacy
 * classes cannot be resolved at compile time because Hadoop 2.x is based on
 * Yarn and thus have different minimr cluster implementations.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class MiniMRClusterCompat {

	private final static Log log = LogFactory.getLog(MiniMRClusterCompat.class);

	/** Class name for hadoops cluster factory */
	private final static String CLASS_FACTORY = "org.apache.hadoop.mapred.MiniMRClientClusterFactory";

	/** Class name for legacy cluster */
	private final static String CLASS_LEGACY = "org.apache.hadoop.mapred.MiniMRCluster";

	/**
	 * Instantiates a minimrcluster.
	 *
	 * @param caller the one who called this method
	 * @param identifier the cluster identifier
	 * @param nodes number of nodes
	 * @param configuration passed configuration
	 * @param fileSystem hdfs filesystem
	 * @param classLoader the class loader
	 * @return the cluster object
	 */
	public static Object instantiateCluster(Class<?> caller, String identifier, int nodes,
			Configuration configuration, FileSystem fileSystem,
			ClassLoader classLoader) {

		log.info("Starting minirmcluster via compat");

		Assert.notNull(caller, "Caller class must be set");
		Assert.notNull(fileSystem, "FileSystem must not be null");

		Object cluster = null;

		Class<?> factoryClass = resolveClass(CLASS_FACTORY, classLoader);
		Class<?> legacyClass = resolveClass(CLASS_LEGACY, classLoader);
		log.info("Cluster classes resolved, factory=" + factoryClass + " legacy=" + legacyClass);

		if (factoryClass != null) {
			Method method = ReflectionUtils.findMethod(factoryClass, "create", Class.class, String.class, int.class,
					Configuration.class);
			cluster = ReflectionUtils.invokeMethod(method, null, caller, identifier, nodes, configuration);
		} else if (legacyClass != null) {
			Constructor<?> constructor = ClassUtils.getConstructorIfAvailable(legacyClass, int.class, String.class, int.class);
			cluster = BeanUtils.instantiateClass(constructor, nodes, fileSystem.getUri().toString(), 1);
		} else {
			log.error("Failed to find or instantiate cluster class");
		}

		if (cluster != null) {
			log.info("Cluster instantiated: " + cluster);
		}

		return cluster;
	}

	/**
	 * Finds and calls lifecycle stop method for
	 * given cluster via reflection.
	 *
	 * @param mrClusterObject the Cluster Object
	 */
	public static void stopCluster(Object mrClusterObject) {
		Assert.notNull(mrClusterObject, "mrClusterObject must not be null");
		log.info("Stopping cluster=" + mrClusterObject);

		Method method = ReflectionUtils.findMethod(mrClusterObject.getClass(), "stop");
		if (method == null) {
			method = ReflectionUtils.findMethod(mrClusterObject.getClass(), "shutdown");
		}

		if (method != null) {
			ReflectionUtils.invokeMethod(method, mrClusterObject);
		} else {
			log.warn("Can't find stop/shutdown method for cluster=" + mrClusterObject);
		}
	}

	/**
	 * Gets the {@link Configuration} from a cluster.
	 *
	 * @param mrClusterObject the Cluster Object
	 * @return the cluster {@link Configuration}
	 */
	public static Configuration getConfiguration(Object mrClusterObject) {
		Assert.notNull(mrClusterObject, "mrClusterObject must not be null");
		log.info("Getting configuration for cluster=" + mrClusterObject);

		Method method = ReflectionUtils.findMethod(mrClusterObject.getClass(), "getConfig");
		if (method == null) {
			method = ReflectionUtils.findMethod(mrClusterObject.getClass(), "createJobConf");
		}

		if (method != null) {
			return (Configuration) ReflectionUtils.invokeMethod(method, mrClusterObject);
		} else {
			log.warn("Can't find configuration for cluster=" + mrClusterObject);
		}

		return null;
	}

	/**
	 * Exception safe method resolving class. Returns
	 * NULL if class is not found.
	 *
	 * @param clazzName fully qualified name of the class
	 * @param classLoader the class loader
	 * @return Resolved class or NULL if not found.
	 */
	private static Class<?> resolveClass(String clazzName, ClassLoader classLoader) {
		Class<?> clazz = null;
		try {
			clazz = ClassUtils.resolveClassName(clazzName, classLoader);
		} catch (IllegalArgumentException e) {
			// not found, not interested
		}
		return clazz;
	}

}
