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
package org.springframework.data.hadoop.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Enumeration;

import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.Assert;

/**
 * Utility class for dealing or location resources for Hadoop jobs.
 * 
 * @author Costin Leau
 * @author Arjen Poutsma
 */
public abstract class ResourceUtils {

	private static final Charset UTF8;

	static {
		try {
			UTF8 = Charset.forName("UTF-8");
		} catch (UnsupportedCharsetException uce) {
			throw new IllegalArgumentException("UTF-8 charset not found!");
		}
	}

	public static Resource findContainingJar(Class<?> clazz) {
		return findContainingJar(clazz.getClassLoader(), clazz.getName());
	}

	public static Resource findContainingJar(ClassLoader loader, String className) {
		String binaryName = className;
		if (!className.endsWith(".class")) {
			binaryName = className.replace(".", "/") + ".class";
		}
			
		try {
			for (Enumeration<URL> urls = loader.getResources(binaryName); urls.hasMoreElements();) {
				URL url = urls.nextElement();
				// remove jar:
				if ("jar".equals(url.getProtocol())) {
					return new UrlResource(decode(url.getPath()).replaceAll("!.*$", ""));
				}
			}
		} catch (IOException ex) {
			throw new IllegalArgumentException("Cannot find jar for class " + className, ex);
		}
		return null;
	}

	public static String decode(String source) {
		Assert.notNull(source, "'source' must not be null");
		int length = source.length();
		ByteArrayOutputStream bos = new ByteArrayOutputStream(length);
		for (int i = 0; i < length; i++) {
			int ch = source.charAt(i);
			if (ch == '%') {
				if ((i + 2) < length) {
					char hex1 = source.charAt(i + 1);
					char hex2 = source.charAt(i + 2);
					int u = Character.digit(hex1, 16);
					int l = Character.digit(hex2, 16);
					bos.write((char) ((u << 4) + l));
					i += 2;
				}
				else {
					throw new IllegalArgumentException("Invalid encoded sequence \"" + source.substring(i) + "\"");
				}
			}
			else {
				bos.write(ch);
			}
		}
		return new String(bos.toByteArray(), UTF8);
	}
}