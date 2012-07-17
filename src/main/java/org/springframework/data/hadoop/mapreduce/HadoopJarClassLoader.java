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
package org.springframework.data.hadoop.mapreduce;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;

/**
 * Dedicated class-loader for running Hadoop jars in-process. It mainly handles the wiring of a custom Configuration class 
 * (through {@link Configuration#addDefaultResource(String)}. Note that other configurations might
 * be created at the same time and the class-loader acts as a filter validating only the configurations loaded through it  
 * 
 * @author Costin Leau
 */
class HadoopJarClassLoader extends ParentLastURLClassLoader {

	private final String configName;
	private final URL configURL;


	HadoopJarClassLoader(URL[] classpath, ClassLoader parent, String configName, URL configURL) {
		super(classpath, parent);
		this.configName = configName;
		this.configURL = configURL;
	}

	@Override
	public URL getResource(String name) {
		// check for custom config name
		if (this.configName.equals(name)) {
			return configURL;
		}
		return super.getResource(name);
	}
}