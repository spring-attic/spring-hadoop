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
package org.springframework.data.hadoop.store.support;

import java.io.UnsupportedEncodingException;

/**
 * Utility methods for store package.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class StoreUtils {

	private static final String utf8 = "UTF-8";

	private static final byte[] delimiter;

	private static final byte[] csv;

	private static final byte[] tab;

	static {
		try {
			delimiter = "\n".getBytes(utf8);
			csv = ",".getBytes(utf8);
			tab = "\t".getBytes(utf8);
		}
		catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("can't find " + utf8 + " encoding");
		}
	}

	/**
	 * Gets the default utf8 delimiter.
	 *
	 * @return the default delimiter
	 */
	public static byte[] getUTF8DefaultDelimiter() {
		return delimiter;
	}

	/**
	 * Gets the default utf8 csv delimiter.
	 *
	 * @return the default csv delimiter
	 */
	public static byte[] getUTF8CsvDelimiter() {
		return csv;
	}

	/**
	 * Gets the default utf8 tab delimiter.
	 *
	 * @return the default tab delimiter
	 */
	public static byte[] getUTF8TabDelimiter() {
		return tab;
	}

}
