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
package org.springframework.data.hadoop.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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

	private static final Charset UTF8 = StandardCharsets.UTF_8;

	/**
	 * Locates the jar (within the classpath) containing the given class.
	 * As this method requires loading a class, it's recommended to use {@link #findContainingJar(ClassLoader, String)}
	 * instead.
	 *
	 * @param clazz the class to look for
	 * @return the containing jar.
	 */
	public static Resource findContainingJar(Class<?> clazz) {
		return findContainingJar(clazz.getClassLoader(), clazz.getName().replace(".", "/") + ".class");
	}

	/**
	 * Locates a jar (within the classpath) containing the given resource.
	 *
	 * @param loader class loader to use for locating the jar
	 * @param resourceName resource to look for
	 * @return the containing jar
	 */
	public static Resource findContainingJar(ClassLoader loader, String resourceName) {
		String binaryName = resourceName;

		try {
			for (Enumeration<URL> urls = loader.getResources(binaryName); urls.hasMoreElements();) {
				URL url = urls.nextElement();
				// remove jar:
				if ("jar".equals(url.getProtocol())) {
					return new UrlResource(decode(url.getPath()).replaceAll("!.*$", ""));
				}
			}
		} catch (IOException ex) {
			throw new IllegalArgumentException("Cannot find jar for class " + resourceName, ex);
		}
		return null;
	}

	/**
	 * Decodes the given encoded source String into an URI. Based on the following
	 * rules:
	 * <ul>
	 * <li>Alphanumeric characters {@code "a"} through {@code "z"},
	 * {@code "A"} through {@code "Z"}, and {@code "0"} through {@code "9"}
	 * stay the same.
	 * <li>Special characters {@code "-"}, {@code "_"}, {@code "."}, and
	 * {@code "*"} stay the same.
	 * <li>All other characters are converted into one or more bytes using the
	 * given encoding scheme. Each of the resulting bytes is written as a
	 * hexadecimal string in the {@code %xy} format.
	 * <li>A sequence "<code>%<i>xy</i></code>" is interpreted as a hexadecimal
	 * representation of the character.
	 * </ul>
	 * @param source the source string
	 * @return the decoded URI
	 * @see java.net.URLDecoder#decode(String, String)
	 */
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