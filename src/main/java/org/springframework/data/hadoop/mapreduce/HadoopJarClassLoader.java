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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.springframework.util.FileCopyUtils;

/**
 * Dedicated class-loader for running Hadoop jars in-process.
 * The purpose of the class is two-fold:
 * 
 * a. to prevent malicious calls (such as System.exit()) - does that through on-the-fly byte-code rewriting.
 * b. to wire in a custom Configuration (through {@link Configuration#addDefaultResource(String)}. Note that other configurations might
 * be created at the same time and the class-loader acts as a filter validating only the configurations loaded through it  
 * 
 * @author Costin Leau
 */
class HadoopJarClassLoader extends ParentLastURLClassLoader {

	HadoopJarClassLoader(URL[] classpath, ClassLoader parent) {
		super(classpath, parent);
	}

	@Override
	public URL getResource(String name) {
		// check for custom resource


		return super.getResource(name);
	}

	@Override
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		InputStream is = getResourceAsStream(name.replace('.', '/') + ".class");
		if (is == null) {
			throw new ClassNotFoundException(name);
		}

		byte[] bytes = null;
		try {
			// Load the raw bytes.
			bytes = FileCopyUtils.copyToByteArray(is);
			// Transform if necessary and use the potentially transformed bytes.
			bytes = transformIfNecessary(name, bytes);
		} catch (IOException ex) {
			throw new ClassNotFoundException("Cannot load resource for class [" + name + "]", ex);
		}

		if (bytes != null) {
			return defineClass(name, bytes, 0, bytes.length);
		}

		throw new ClassNotFoundException(name);
	}

	private byte[] transformIfNecessary(String name, byte[] bytes) {
		// replace System.exit
		return bytes;
	}
}