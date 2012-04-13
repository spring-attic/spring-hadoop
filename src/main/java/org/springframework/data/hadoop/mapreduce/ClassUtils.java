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
import java.net.URL;

import org.apache.hadoop.util.Tool;
import org.springframework.beans.BeanUtils;
import org.springframework.core.io.Resource;

/**
 * Class-related utilities.
 * 
 * @author Costin Leau
 */
abstract class ClassUtils {

	@SuppressWarnings("unchecked")
	public static <T> T loadClassParentLast(Resource jar, ClassLoader parentClassLoader, String className) {
		ClassLoader cl = createParentLastClassLoader(jar, parentClassLoader);
		Class<? extends Tool> toolClass = (Class<? extends Tool>) org.springframework.util.ClassUtils.resolveClassName(className, cl);
		return (T) BeanUtils.instantiateClass(toolClass);
	}
	
	public static ClassLoader createParentLastClassLoader(Resource jar, ClassLoader parentClassLoader) {
		ClassLoader cl = null;
		
		// sanity check
		if (parentClassLoader == null) {
			parentClassLoader = org.springframework.util.ClassUtils.getDefaultClassLoader();
			cl = parentClassLoader;
		}
		
		// check if a custom CL is needed
		if (jar != null) {
			try {
				cl = new ParentLastURLClassLoader(new URL[] { jar.getURL() }, parentClassLoader);
			} catch (IOException e) {
				throw new IllegalStateException("Cannot open jar file", e);
			}
		}
		
		return cl;
	}
}
