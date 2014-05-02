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
package org.springframework.data.hadoop.mapreduce;

import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.configuration.JobConfUtils;
import org.springframework.data.hadoop.mapreduce.ExecutionUtils.ExitTrapped;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Base configuration class for executing custom Hadoop code (such as Tool or Jar).
 * 
 * @author Costin Leau
 */
abstract class HadoopCodeExecutor<T> extends JobGenericOptions implements InitializingBean, BeanClassLoaderAware {

	String[] arguments;
	Configuration configuration;
	T target;
	String targetClassName;
	Properties properties;
	Resource jar;
	private ClassLoader beanClassLoader;
	private boolean closeFs = true;

	// do the JRE leak prevention, once per class-loader
	static {
		ExecutionUtils.preventJreTcclLeaks();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.isTrue(target != null || StringUtils.hasText(targetClassName) || (jar != null && jar.exists()),
				"a target instance, class name or a Jar (with Main-Class) is required");
	}


	protected int runCode() throws Exception {
		// merge configuration options
		final Configuration cfg = resolveConfiguration();

		// resolve target object
		final Class<T> type = resolveTargetClass(cfg);
		final T target = resolveTargetObject(type);

		// setup the invocation context
		Thread th = Thread.currentThread();
		ClassLoader oldTccl = th.getContextClassLoader();

		log.info("Invoking [" + (target != null ? target : type) + "] "
				+ (jar != null ? "from jar [" + jar.getURI() + "]" : "") + " with args [" + Arrays.toString(arguments)
				+ "]");

		ClassLoader newCL = cfg.getClassLoader();
		boolean isJarCL = newCL instanceof ParentLastURLClassLoader;
		try {
			ExecutionUtils.disableSystemExitCall();
			if (isJarCL) {
				ExecutionUtils.preventHadoopLeaks(beanClassLoader);
			}

			//ExecutionUtils.earlyLeaseDaemonInit(cfg);

			th.setContextClassLoader(newCL);

			if (StringUtils.hasText(user)) {
				UserGroupInformation ugi = UserGroupInformation.createProxyUser(user,
						UserGroupInformation.getLoginUser());

				return ugi.doAs(new PrivilegedExceptionAction<Integer>() {
					@Override
					public Integer run() throws Exception {
						return invokeTarget(cfg, target, type, arguments);
					}
				});
			}
			else {
				return invokeTarget(cfg, target, type, arguments);
			}
		} finally {
			ExecutionUtils.enableSystemExitCall();
			th.setContextClassLoader(oldTccl);

			if (isJarCL) {
				if (closeFs) {
					ExecutionUtils.shutdownFileSystem(cfg);
				}
				ExecutionUtils.patchLeakedClassLoader(newCL, oldTccl);
			}
		}
	}


	protected Configuration resolveConfiguration() throws Exception {
		Configuration cfg = JobConfUtils.createFrom(configuration, properties);
		// add the jar if present
		if (jar != null) {
			String jarUrl = jar.getURL().toString();
			if (log.isTraceEnabled()) {
				log.trace("Setting Configuration Jar URL to [" +jarUrl+"]");
			}

			cfg.set("mapred.jar", jarUrl);
		}

		buildGenericOptions(cfg);
		return cfg;
	}

	@SuppressWarnings("unchecked")
	protected Class<T> resolveTargetClass(Configuration cfg) throws Exception {
		ClassLoader cl = beanClassLoader;
		// no target set - we might need to load one from the custom jar
		if (target == null) {
			cl = createClassLoaderForJar(jar, cl, cfg);

			// make sure to pass this to the Configuration
			cfg.setClassLoader(cl);

			if (jar != null) {
				if (log.isTraceEnabled()) {
					log.trace("Creating custom classloader " + cl);
				}

				// fall-back to main
				if (!StringUtils.hasText(targetClassName)) {
					String mainClass = ExecutionUtils.mainClass(jar);
					Assert.notNull(mainClass, "no target class specified and no Main-Class available");
					targetClassName = mainClass;

					if (log.isDebugEnabled()) {
						log.debug("Discovered Main-Class [" + mainClass + "]");
					}
				}
			}
			else {
				Assert.hasText(targetClassName, "No target object, class or jar specified - execution aborted");
			}
			return loadClass(targetClassName, cl);
		}

		return (Class<T>) target.getClass();
	}

	protected T resolveTargetObject(Class<T> type) {
		return (target != null ? target : BeanUtils.instantiateClass(type));
	}

	protected ClassLoader createClassLoaderForJar(Resource jar, ClassLoader parentCL, Configuration cfg) {
		return ExecutionUtils.createParentLastClassLoader(jar, parentCL, cfg);
	}

	@SuppressWarnings("unchecked")
	protected Class<T> loadClass(String className, ClassLoader cl) {
		return (Class<T>) ClassUtils.resolveClassName(className, cl);
	}

	private Integer invokeTarget(Configuration cfg, T target, Class<T> targetClass, String[] args) throws Exception {
		preExecution(cfg);
		try {
			Object result = invokeTargetObject(cfg, target, targetClass, args);
			if (result instanceof Integer) {
				return (Integer) result;
			}

			return Integer.valueOf(0);
		} catch (ExitTrapped trap) {
			log.debug("Code exited");
			return trap.getExitCode();
		} finally {
			postExecution(cfg);
		}
	}

	protected void preExecution(Configuration cfg) {
		// no-op
	}

	protected void postExecution(Configuration cfg) {
		// no-op
	}

	protected abstract Object invokeTargetObject(Configuration cfg, T target, Class<T> targetClass, String[] args)
			throws Exception;


	/**
	 * Sets the target code jar.
	 * 
	 * @param jar target jar
	 */
	public void setJar(Resource jar) {
		this.jar = jar;
	}

	/**
	 * Sets the arguments.
	 *
	 * @param arguments The arguments to set.
	 */
	public void setArguments(String... arguments) {
		this.arguments = arguments;
	}

	/**
	 * Sets the configuration.
	 *
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Sets the properties.
	 *
	 * @param properties The properties to set.
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	/**
	 * Indicates whether or not to close the Hadoop file-systems
	 * resulting from the custom code execution.
	 * Default is true. Turn this to false if the code reuses the same
	 * file-system used by the rest of the application.
	 *
	 * @param closeFs the new close fs
	 */
	public void setCloseFs(boolean closeFs) {
		this.closeFs = closeFs;
	}

	/**
	 * Sets the target class.
	 *
	 * @param target The target class to set.
	 */
	void setTargetObject(T target) {
		Assert.isNull(targetClassName, "a target class already set");
		this.target = target;
	}

	/**
	 * Sets the target class name.
	 *
	 * @param targetClassName the target class name.
	 */
	void setTargetClassName(String targetClassName) {
		this.targetClassName = targetClassName;
	}
}