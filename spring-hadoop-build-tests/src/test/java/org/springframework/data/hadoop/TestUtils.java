/*
 * Copyright 2011-2014 the original author or authors.
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
package org.springframework.data.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;

/**
 * Testing utilities.
 *
 * @author Costin Leau
 * @author Janne Valkealahti
 *
 */
public abstract class TestUtils {

	public static Resource mkdir(Configuration cfg, String dir) {
		HdfsResourceLoader loader = new HdfsResourceLoader(cfg);
		try {
			Resource resource = loader.getResource(dir);
			FileSystem fs = ((HdfsResourceLoader) loader).getFileSystem();
			fs.mkdirs(new Path(resource.getURI()));
			return loader.getResource(dir);
		} catch (IOException ex) {
			try {
				loader.close();
			} catch (IOException ioe) {
			}
			throw new IllegalArgumentException(ex);
		}

	}

	public static Resource writeToFS(Configuration cfg, String filename) {
		return writeToFS(new HdfsResourceLoader(cfg), filename);
	}

	public static Resource writeToFS(HdfsResourceLoader loader, String filename) {
		try {
			Resource resource = loader.getResource(filename);
			//System.out.println("Writing resource " + resource.getURI());
			//WritableResource wr = (WritableResource) resource;

			FileSystem fs = ((HdfsResourceLoader) loader).getFileSystem();

			byte[] bytes = filename.getBytes();
			OutputStream out = fs.create(new Path(resource.getURI()));
			out.write(bytes);
			out.flush();
			out.close();

			return loader.getResource(filename);
		} catch (IOException ex) {
			throw new IllegalArgumentException(ex);
		}
	}

	public static boolean compareStreams(InputStream expected, InputStream actual) {
		try {
			int i = 0;
			while ((i = expected.read()) != -1) {
				int j = actual.read();
				if (i != j) {
					return false;
				}
			}
			return true;
		} catch (IOException ex) {
			return false;
		} finally {
			try {
				expected.close();
			} catch (Exception ex) {
			}
			try {
				actual.close();
			} catch (Exception ex) {
			}
		}
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

}