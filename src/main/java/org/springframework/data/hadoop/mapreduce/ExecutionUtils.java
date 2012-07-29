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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;


/**
 * Code execution utilities.
 * 
 * @author Costin Leau
 * @author Jarred Li
 */
// NOTE: jars with nested /classes/ are supported as well but this functionality is disabled
// as it seems to have not been used in hadoop.
abstract class ExecutionUtils {

	private static final Log log = LogFactory.getLog(ExecutionUtils.class);

	static class ExitTrapped extends Error {

		private int exitCode;

		ExitTrapped(String permissionName) {
			// handle non-Sun JDKs 
			int hasDot = permissionName.indexOf(".");
			this.exitCode = Integer.valueOf((hasDot > 0 ? permissionName.substring(hasDot + 1) : permissionName.substring(7)));
		}

		public int getExitCode() {
			return exitCode;
		}
	}

	private static SecurityManager oldSM = null;

	static void disableSystemExitCall() {
		final SecurityManager securityManager = new SecurityManager() {
			
			@Override
			public void checkPermission(Permission permission) {
				String name = permission.getName();
				if (name.startsWith("exitVM")) {
					throw new ExitTrapped(name);
				}
			}
		};

		oldSM = System.getSecurityManager();
		System.setSecurityManager(securityManager);
	}

	static void enableSystemExitCall() {
		System.setSecurityManager(oldSM);
	}

	static ClassLoader createParentLastClassLoader(Resource jar, ClassLoader parentClassLoader, Configuration cfg) {
		ClassLoader cl = null;

		// sanity check
		if (parentClassLoader == null) {
			parentClassLoader = ClassUtils.getDefaultClassLoader();
			cl = parentClassLoader;
		}

		// check if a custom CL is needed
		if (jar != null) {
			// check if unjarring is required (it's a legacy JAR)
			try {
				if (isLegacyJar(jar)) {
					URL[] extractedURLs = expandedJarClassPath(jar, cfg);
					cl = new ParentLastURLClassLoader(extractedURLs, parentClassLoader);
				}
				else {
					cl = new ParentLastURLClassLoader(new URL[] { jar.getURL() }, parentClassLoader);
				}

			} catch (IOException e) {
				throw new IllegalStateException("Cannot open jar file", e);
			}
		}

		return cl;
	}

	private static boolean isLegacyJar(Resource jar) throws IOException {
		JarInputStream jis = new JarInputStream(jar.getInputStream());
		JarEntry entry = null;
		try {
			while ((entry = jis.getNextJarEntry()) != null) {
				String name = entry.getName();
				if (name.startsWith("lib/") //|| name.startsWith("classes/")
				) {
					return true;
				}
			}
		} finally {
			IOUtils.closeStream(jis);
		}
		return false;
	}

	private static URL[] expandedJarClassPath(Resource jar, Configuration cfg) throws IOException {
		// detect base dir
		File baseDir = detectBaseDir(cfg);

		// expand the jar
		unjar(jar, baseDir);

		// build classpath
		List<URL> cp = new ArrayList<URL>();
		cp.add(new File(baseDir + "/").toURI().toURL());

		//cp.add(new File(baseDir + "/classes/").toURI().toURL());
		File[] libs = new File(baseDir, "lib").listFiles();
		if (libs != null) {
			for (int i = 0; i < libs.length; i++) {
				cp.add(libs[i].toURI().toURL());
			}
		}

		return cp.toArray(new URL[cp.size()]);
	}


	private static File detectBaseDir(Configuration cfg) throws IOException {
		File tmpDir = null;

		if (cfg != null) {
			tmpDir = new File(cfg.get("hadoop.tmp.dir"));
			tmpDir.mkdirs();
			if (!tmpDir.isDirectory()) {
				tmpDir = null;
			}
		}

		final File workDir = File.createTempFile("hadoop-unjar", "", tmpDir);
		workDir.delete();
		workDir.mkdirs();

		return workDir;
	}


	private static void unjar(Resource jar, File baseDir) throws IOException {
		JarInputStream jis = new JarInputStream(jar.getInputStream());
		JarEntry entry = null;
		try {
			while ((entry = jis.getNextJarEntry()) != null) {
				if (!entry.isDirectory()) {
					File file = new File(baseDir, entry.getName());
					if (!file.getParentFile().mkdirs()) {
						if (!file.getParentFile().isDirectory()) {
							throw new IOException("Mkdirs failed to create " + file.getParentFile().toString());
						}
					}
					OutputStream out = new FileOutputStream(file);
					try {
						byte[] buffer = new byte[8192];
						int i;
						while ((i = jis.read(buffer)) != -1) {
							out.write(buffer, 0, i);
						}
					} finally {
						IOUtils.closeStream(out);
					}
				}
			}
		} finally {
			IOUtils.closeStream(jis);
		}
	}

	static String mainClass(Resource jar) throws IOException {
		JarInputStream jis = new JarInputStream(jar.getInputStream());
		try {
			Manifest mf = jis.getManifest();
			if (mf != null) {
				String main = mf.getMainAttributes().getValue("Main-Class");
				if (StringUtils.hasText(main)) {
					return main.replace("/", ".");
				}
			}
			return null;
		} finally {
			IOUtils.closeStream(jis);
		}
	}

	/**
	 * Leak-preventing method analyzing the threads started by the JVM which hold a reference
	 * to a classloader that should be reclaimed. 
	 * 
	 * @param leakedClassLoader
	 * @param replacementClassLoader
	 */
	static void replaceLeakedClassLoader(ClassLoader leakedClassLoader, ClassLoader replacementClassLoader) {
		Set<Thread> threadSet = Thread.getAllStackTraces().keySet();

		for (Thread thread : threadSet) {
			ClassLoader cl = thread.getContextClassLoader();
			// do identity check to prevent expensive (and potentially dangerous) equals()
			if (leakedClassLoader == cl) {
				log.warn("Trying to patch leaked cl [" + leakedClassLoader + "] in thread " + thread);
				thread.setContextClassLoader(replacementClassLoader);
			}
		}
	}
}