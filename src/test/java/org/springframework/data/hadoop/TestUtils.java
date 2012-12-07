/*
 * Copyright 2011 the original author or authors.
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
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.data.hadoop.util.PermissionUtils;

/**
 * @author Costin Leau
 */
public class TestUtils {

	/**
	 * Hack to allow Hadoop client to run on windows.
	 * Since Hadoop 1.0.x this fails since Hadoop expects certain permissions to be set, which it cannot on Windows/NTFS.
	 * To overcome this, the hack changes the permissions required by the staging process.
	 * However this causes the job to fail when interacting with a remote cluster since the statging permission set between the client
	 * and the server are different.
	 * The error message however doesn't properly shows that (it actually points to the permissions available on the server though it uses
	 * the client ones). The solution is to _not_ use the hacked permissions but rather the original set.
	 * 
	 * Note that if the client has created a folder already with the hacked set, Hadoop will complain - the solution is to remove the folder.
	 *
	 * 
	 * This method tries to address this by enabling the permissions only if the 'hadoop.jt' property is non-local. Additionally the boostrapping hadoop-context
	 * features a script which removes the staging folder to avoid permissions mismatches.
	 */
	public static void hackHadoopStagingOnWin() {
		// check test properties 
		try {
			Properties testProperties = PropertiesLoaderUtils.loadProperties(new ClassPathResource("/test.properties"));
			if (testProperties != null && "local".equals(testProperties.get("hadoop.jt"))) {
				if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
					System.out.println("[TestUtils] Running a local Hadoop JT on !Windows! - hacking Hadoop Permissions...");
					PermissionUtils.hackHadoopStagingOnWin();
				}
			}
		} catch (IOException ex) {
			// ignore
		}
	}

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
}