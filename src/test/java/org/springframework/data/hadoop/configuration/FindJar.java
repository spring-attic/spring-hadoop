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
package org.springframework.data.hadoop.configuration;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

import org.junit.Test;
import org.slf4j.Logger;
import org.springframework.data.hadoop.util.ResourceUtils;

import cascading.flow.BaseFlow;
import cascading.flow.hadoop.HadoopFlow;

/**
 * @author Costin Leau
 */
public class FindJar {

	private static String findContainingJar(Class my_class) {
		ClassLoader loader = my_class.getClassLoader();
		String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
		try {
			for (Enumeration itr = loader.getResources(class_file); itr.hasMoreElements();) {
				URL url = (URL) itr.nextElement();
				if ("jar".equals(url.getProtocol())) {
					String toReturn = url.getPath();
					if (toReturn.startsWith("file:")) {
						toReturn = toReturn.substring("file:".length());
					}
					toReturn = URLDecoder.decode(toReturn, "UTF-8");
					return toReturn.replaceAll("!.*$", "");
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	@Test
	public void testFindCascadingJars() throws Exception {
		System.out.println("Cascading Hadoop " + ResourceUtils.findContainingJar(HadoopFlow.class));
		System.out.println("Cascading Core " + ResourceUtils.findContainingJar(BaseFlow.class));
		System.out.println("slf4j " + ResourceUtils.findContainingJar(Logger.class));
	}

}
