/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.store;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertNull;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.util.ReflectionUtils;

/**
 * Utilities for tests.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class TestUtils {

	public static void writeData(DataStoreWriter<List<String>> writer, List<List<String>> data) throws IOException {
		writeData(writer, data, true);
	}

	public static void writeData(DataStoreWriter<List<String>> writer, List<List<String>> data, boolean close) throws IOException {
		for (List<String> d : data) {
			writer.write(d);
		}
		writer.flush();
		if (close) {
			writer.close();
		}
	}

	public static void writeData(DataStoreWriter<String> writer, String[] data) throws IOException {
		writeData(writer, data, true);
	}

	public static void writeData(DataStoreWriter<String> writer, String[] data, boolean close) throws IOException {
		writeData(writer, data, close, true);
	}

	public static void writeData(DataStoreWriter<String> writer, String[] data, boolean close, boolean flush) throws IOException {
		for (String line : data) {
			writer.write(line);
		}
		if (flush) {
			writer.flush();
		}
		if (close) {
			writer.close();
		}
	}

	public static void writeData(DataStoreWriter<byte[]> writer, byte[][] data, boolean close) throws IOException {
		for (byte[] d : data) {
			writer.write(d);
		}
		writer.flush();
		if (close) {
			writer.close();
		}
	}

	public static void readDataAndAssert(DataStoreReader<String> reader, String[] expected) throws IOException {
		String line = null;
		int count = 0;
		while ((line = reader.read()) != null) {
			assertThat(count, lessThan(expected.length));
			assertThat(line, is(expected[count++]));
		}
		assertThat(count, is(expected.length));
		line = reader.read();
		assertNull("Expected null, got '" + line + "'", line);
	}

	public static List<String> readData(DataStoreReader<String> reader) throws IOException {
		ArrayList<String> ret = new ArrayList<String>();
		String line = null;
		while ((line = reader.read()) != null) {
			ret.add(line);
		}
		return ret;
	}

	public static List<List<String>> readDataList(DataStoreReader<List<String>> reader) throws IOException {
		List<List<String>> ret = new ArrayList<List<String>>();
		List<String> line = null;
		while ((line = reader.read()) != null) {
			ret.add(line);
		}
		return ret;
	}

	@SuppressWarnings("unchecked")
	public static <T> T readField(String name, Object target) throws Exception {
		Field field = null;
		Class<?> clazz = target.getClass();
		do {
			try {
				field = clazz.getDeclaredField(name);
			} catch (Exception ex) {
			}

			clazz = clazz.getSuperclass();
		} while (field == null && !clazz.equals(Object.class));

		if (field == null)
			throw new IllegalArgumentException("Cannot find field '" + name + "' in the class hierarchy of "
					+ target.getClass());
		field.setAccessible(true);
		return (T) field.get(target);
	}

	@SuppressWarnings("unchecked")
	public static <T> T callMethod(String name, Object target) throws Exception {
		Class<?> clazz = target.getClass();
		Method method = ReflectionUtils.findMethod(clazz, name);

		if (method == null)
			throw new IllegalArgumentException("Cannot find method '" + method + "' in the class hierarchy of "
					+ target.getClass());
		method.setAccessible(true);
		return (T) ReflectionUtils.invokeMethod(method, target);
	}

	public static void setField(String name, Object target, Object value) throws Exception {
		Field field = null;
		Class<?> clazz = target.getClass();
		do {
			try {
				field = clazz.getDeclaredField(name);
			} catch (Exception ex) {
			}

			clazz = clazz.getSuperclass();
		} while (field == null && !clazz.equals(Object.class));

		if (field == null)
			throw new IllegalArgumentException("Cannot find field '" + name + "' in the class hierarchy of "
					+ target.getClass());
		field.setAccessible(true);
		field.set(target, value);
	}

	@SuppressWarnings("unchecked")
	public static <T> T callMethod(String name, Object target, Object[] args, Class<?>[] argsTypes) throws Exception {
		Class<?> clazz = target.getClass();
		Method method = ReflectionUtils.findMethod(clazz, name, argsTypes);

		if (method == null)
			throw new IllegalArgumentException("Cannot find method '" + method + "' in the class hierarchy of "
					+ target.getClass());
		method.setAccessible(true);
		return (T) ReflectionUtils.invokeMethod(method, target, args);
	}

	public static void close(Closeable closeable) {
		try {
			closeable.close();
		} catch (Exception e) {
		}
	}

	@SuppressWarnings("resource")
	public static void printLsR(String path, org.apache.hadoop.conf.Configuration configuration) {
		FsShell shell = new FsShell(configuration);
		for (FileStatus s : shell.ls(true, path)) {
			System.out.println(">>> " + s);
		}
	}

}
