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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

/**
 * Customized executor for Hadoop jars.
 * 
 * @author Costin Leau
 */
public abstract class JarExecutor extends HadoopCodeExecutor<Object> {

	private File savedConfiguration;
	private String configName;

	// create a custom CL to re-inject the custom Hadoop config
	@Override
	protected ClassLoader createClassLoaderForJar(Resource jar, ClassLoader parentCL, Configuration cfg) {
		return new ClassLoader(ExecutionUtils.createParentLastClassLoader(jar, parentCL, cfg)) {
			@Override
			public URL getResource(String name) {
				// check for custom config name
				if (savedConfiguration != null && configName != null && configName.equals(name)) {
					try {
						return savedConfiguration.toURI().toURL();
					} catch (IOException ex) {
						throw new IllegalStateException("Cannot add custom configuration", ex);
					}
				}
				return super.getResource(name);
			}
		};
	}

	@Override
	protected Object resolveTargetObject(Class<Object> type) {
		// for jar, the main class doesn't have to be instantiated
		return null;
	}

	@Override
	protected Object invokeTargetObject(Configuration cfg, Object target, Class<Object> targetClass, String[] args)
			throws Exception {
		Method main = ReflectionUtils.findMethod(targetClass, "main", String[].class);
		return ReflectionUtils.invokeMethod(main, null, new Object[] { args });
	}

	// inject the config
	@Override
	protected void preExecution(Configuration cfg) {
		// generate a name
		configName = "Custom-cfg-for- " + jar + "-" + UUID.randomUUID();
		try {
			savedConfiguration = File.createTempFile("SHDP-jar-cfg-", null);
			cfg.writeXml(new FileOutputStream(savedConfiguration));
			// don't use addDefaultResource because it has side-effects
			//Configuration.addDefaultResource(configName);
			defaultResources().add(configName);
		} catch (IOException ex) {
			throw new IllegalArgumentException("Cannot set custom configuration", ex);
		}
	}

	// do configuration clean-up
	@Override
	protected void postExecution(Configuration cfg) {
		defaultResources().remove(configName);

		// delete the file
		savedConfiguration.delete();
		savedConfiguration = null;
		configName = null;
	}

	@SuppressWarnings("unchecked")
	private List<String> defaultResources(){
		// reflection hack to remove default resource
		Field f = ReflectionUtils.findField(Configuration.class, "defaultResources");
		ReflectionUtils.makeAccessible(f);
		return (List<String>) ReflectionUtils.getField(f, null);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.isTrue(jar != null && jar.exists(), "jar location [" + jar + "] not found");
	}

	/**
	 * Sets the target class by name.
	 *
	 * @param className the target class name
	 */
	public void setMainClass(String className) {
		setTargetClassName(className);
	}
}