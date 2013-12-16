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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.util.Assert;

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

	public static final int BUFFER_SIZE = 4096;

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

	/**
	 * Copy the contents of the given InputStream to the given DataStoreWriter.
	 * Closes stream and writer when done.
	 *
	 * @param in the input stream
	 * @param out the data store writer
	 * @return the number of bytes copied
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static int copy(InputStream in, DataStoreWriter<byte[]> out) throws IOException {
		Assert.notNull(in, "No InputStream specified");
		Assert.notNull(out, "No DataStoreWriter specified");
		try {
			return copyStream(in, out);
		} finally {
			try {
				in.close();
			} catch (IOException ex) {
			}
			try {
				out.close();
			} catch (IOException ex) {
			}
		}
	}

	/**
	 * Copy the contents of the given InputStream to the given DataStoreWriter.
	 * Does not close stream or writer when done.
	 *
	 * @param in the input stream
	 * @param out the data store writer
	 * @return the number of bytes copied
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static int copyStream(InputStream in, DataStoreWriter<byte[]> out) throws IOException {
		Assert.notNull(in, "No InputStream specified");
		Assert.notNull(out, "No DataStoreWriter specified");
		int byteCount = 0;
		byte[] buffer = new byte[BUFFER_SIZE];
		int bytesRead = -1;
		while ((bytesRead = in.read(buffer)) != -1) {
			if (bytesRead < BUFFER_SIZE) {
				// handling case when we wanted to write
				// less than a buffer size
				byte[] buf = new byte[bytesRead];
				System.arraycopy(buffer, 0, buf, 0, bytesRead);
				out.write(buf);
			} else {
				out.write(buffer);
			}
			byteCount += bytesRead;
		}
		out.flush();
		return byteCount;
	}

}
