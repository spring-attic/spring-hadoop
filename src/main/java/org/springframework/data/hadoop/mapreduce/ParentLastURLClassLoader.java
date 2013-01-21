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

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * Extension for {@link URLClassLoader} that uses a parent-last (or child first) delegation.
 *  
 * @author Costin Leau
 */
class ParentLastURLClassLoader extends URLClassLoader {

	private final ClassLoader system;

	public ParentLastURLClassLoader(URL[] classpath, ClassLoader parent) {
		super(classpath, parent);
		ClassLoader sys = getSystemClassLoader();

		while (sys.getParent() != null) {
			sys = sys.getParent();
		}

		system = sys;
	}


	@Override
	protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
		// First, check if the class has already been loaded
		Class<?> c = findLoadedClass(name);
		if (c == null) {
			// always check system class loader (for jvm classes & co)
			if (system != null) {
				try {
					c = system.loadClass(name);
				} catch (ClassNotFoundException ignored) {
				}
			}
			if (c == null) {
				try {
					// load local
					c = findClass(name);
				} catch (ClassNotFoundException e) {
					// fall back to parent
					c = super.loadClass(name, resolve);
				}
			}
		}
		if (resolve) {
			resolveClass(c);
		}
		return c;
	}

	@Override
	public URL getResource(String name) {
		// same delegation as with load class
		URL url = null;
		if (system != null) {
			url = system.getResource(name);
		}
		if (url == null) {
			url = findResource(name);
			if (url == null) {
				url = super.getResource(name);
			}
		}
		return url;
	}

	@Override
	public Enumeration<URL> getResources(String name) throws IOException {
		List<URL> urls = new ArrayList<URL>();
		if (system != null) {
			urls.addAll(Collections.list(system.getResources(name)));
		}
		urls.addAll(Collections.list(findResources(name)));

		ClassLoader parent = getParent();
		if (parent != null) {
			urls.addAll(Collections.list(parent.getResources(name)));
		}

		return Collections.enumeration(urls);
	}
	
    @Override
    public String toString() {
		StringBuilder sb = new StringBuilder("ParentLastURLCL\r\nURLs: ");
		sb.append(Arrays.asList(getURLs()));
		sb.append("\nParent CL: ");
		sb.append(getParent());
		sb.append("\nSystem CL: ");
		sb.append(system);
		sb.append("\n");
        return (sb.toString());
    }
}