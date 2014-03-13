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
package org.springframework.data.hadoop.mapreduce;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.security.Permission;
import java.security.Policy;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.Counters;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
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

	@SuppressWarnings("serial")
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

	private static Field CLASS_CACHE;
	private static Method UTILS_CONSTRUCTOR_CACHE;

	static {
		CLASS_CACHE = ReflectionUtils.findField(Configuration.class, "CACHE_CLASS");
		if (CLASS_CACHE != null) {
			ReflectionUtils.makeAccessible(CLASS_CACHE);
		}

		UTILS_CONSTRUCTOR_CACHE = ReflectionUtils.findMethod(org.apache.hadoop.util.ReflectionUtils.class, "clearCache");
		ReflectionUtils.makeAccessible(UTILS_CONSTRUCTOR_CACHE);
	}

	private static final Set<String> JVM_THREAD_NAMES = new HashSet<String>();

	static {
		JVM_THREAD_NAMES.add("system");
		JVM_THREAD_NAMES.add("RMI Runtime");
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

	@SuppressWarnings("resource")
	private static boolean isLegacyJar(Resource jar) throws IOException {
		JarInputStream jis = new JarInputStream(jar.getInputStream());
		JarEntry entry = null;
		try {
			while ((entry = jis.getNextJarEntry()) != null) {
				String name = entry.getName();
				if (name.startsWith("lib/")) {//|| name.startsWith("classes/")
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


	@SuppressWarnings("resource")
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
	 * Utility method used before invoking custom code for preventing custom classloader, set as the Thread
	 * context class-loader, to leak (typically through JDK classes).
	 */
	static void preventJreTcclLeaks() {
		if (log.isDebugEnabled()) {
			log.debug("Preventing JRE TCCL leaks");
		}

		// get the root CL to be used instead
		ClassLoader sysLoader = ClassLoader.getSystemClassLoader();

		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		try {
			// set the sysCL as the TCCL
			Thread.currentThread().setContextClassLoader(sysLoader);

			//
			// Handle security
			//

			// Policy holds the TCCL as static
			ClassUtils.resolveClassName("javax.security.auth.Policy", sysLoader);
			// since the class init may be lazy, call the method directly
			Policy.getPolicy();
			// Configuration holds the TCCL as static
			// call method with minimal side-effects (since just doing class loading doesn't seem to trigger the static init)
			try {
				javax.security.auth.login.Configuration.getInstance(null, null, (String) null);
			} catch (Exception ex) {
				// ignore
			}
			// seems to cause side-effects/exceptions
			// javax.security.auth.login.Configuration.getConfiguration();
			java.security.Security.getProviders();

			// load the JDBC drivers (used by Hive and co)
			DriverManager.getDrivers();
			// Initialize
			// sun.awt.AppContext.getAppContext()
			ImageIO.getCacheDirectory();

		} finally {
			Thread.currentThread().setContextClassLoader(cl);
		}
	}

	/**
	 * Utility for doing static init for preventing Hadoop leaks during initialization (mainly based on TCCL).
	 */
	static void preventHadoopLeaks(ClassLoader hadoopCL) {

		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		try {
			// set the sysCL as the TCCL
			Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());

			// fix org.apache.hadoop.mapred.Counters#MAX_COUNTER_LIMIT
			// calling constructor since class loading is lazy
			new Counters();
		} finally {
			Thread.currentThread().setContextClassLoader(cl);
		}
	}

	/**
	 * Leak-preventing method analyzing the threads started by the JVM which hold a reference
	 * to a classloader that should be reclaimed.
	 *
	 * @param leakedClassLoader
	 * @param replacementClassLoader
	 */
	static void patchLeakedClassLoader(ClassLoader leakedClassLoader, ClassLoader replacementClassLoader) {
		if (log.isDebugEnabled()) {
			log.debug("Patching TCCL leaks");
		}

		replaceTccl(leakedClassLoader, replacementClassLoader);

		fixHadoopReflectionUtilsLeak(leakedClassLoader);
		fixHadoopReflectionUtilsLeak();
		cleanHadoopLocalDirAllocator();
	}

	/**
	 * Clean the LocalDirAllocator#contexts
	 */
	private static void cleanHadoopLocalDirAllocator() {
		Field field = ReflectionUtils.findField(LocalDirAllocator.class, "contexts");
		ReflectionUtils.makeAccessible(field);
		Map<?, ?> contexts = (Map<?, ?>) ReflectionUtils.getField(field, null);
		if (contexts != null) {
			contexts.clear();
		}
	}

	private static void fixHadoopReflectionUtilsLeak(ClassLoader leakedClassLoader) {
		// replace Configuration#CLASS_CACHE in Hadoop 2.0 which prevents CL from being recycled
		// this is a best-effort really as the leak can occur again - see HADOOP-8632

		// only available on Hadoop-2.0/CDH4
		if (CLASS_CACHE == null) {
			return;
		}

		Map<?, ?> cache = (Map<?, ?>) ReflectionUtils.getField(CLASS_CACHE, null);
		cache.remove(leakedClassLoader);
	}

	private static void fixHadoopReflectionUtilsLeak() {
		// org.apache.hadoop.util.ReflectionUtils.clearCache();
		ReflectionUtils.invokeMethod(UTILS_CONSTRUCTOR_CACHE, null);
	}


	private static void replaceTccl(ClassLoader leakedClassLoader, ClassLoader replacementClassLoader) {
		for (Thread thread : threads()) {
			if (thread != null) {
				ClassLoader cl = thread.getContextClassLoader();
				// do identity check to prevent expensive (and potentially dangerous) equals()
				if (leakedClassLoader == cl) {
					log.warn("Trying to patch leaked cl [" + leakedClassLoader + "] in thread [" + thread + "]");
					ThreadGroup tg = thread.getThreadGroup();
					// it's a JVM thread so use the System ClassLoader always
					boolean debug = log.isDebugEnabled();
					if (tg != null && JVM_THREAD_NAMES.contains(tg.getName())) {
						thread.setContextClassLoader(ClassLoader.getSystemClassLoader());
						if (debug) {
							log.debug("Replaced leaked cl in thread [" + thread + "] with system classloader");
						}
					}
					else {
						thread.setContextClassLoader(replacementClassLoader);
						if (debug) {
							log.debug("Replaced leaked cl in thread [" + thread + "] with " + replacementClassLoader);
						}
					}
				}
			}
		}
	}

	/**
	 * Most jars don't close the file system.
	 *
	 * @param cfg
	 */
	static void shutdownFileSystem(Configuration cfg) {
		FileSystem fs;
		try {
			fs = FileSystem.get(cfg);
			if (fs != null) {
				fs.close();
			}
		} catch (Exception ex) {
		}
		try {
			fs = FileSystem.getLocal(cfg);
			if (fs != null) {
				fs.close();
			}
		} catch (Exception ex) {
		}
	}

	/**
	 * Returns the threads running inside the current JVM.
	 *
	 * @return running threads
	 */
	static Thread[] threads() {
		// Could have used the code below but it tends to be somewhat ineffective and slow
		// Set<Thread> threadSet = Thread.getAllStackTraces().keySet();

		// Get the current thread group
		ThreadGroup tg = Thread.currentThread().getThreadGroup();
		// Find the root thread group
		while (tg.getParent() != null) {
			tg = tg.getParent();
		}

		int threadCountGuess = tg.activeCount() + 50;
		Thread[] threads = new Thread[threadCountGuess];
		int threadCountActual = tg.enumerate(threads);
		// Make sure we don't miss any threads
		while (threadCountActual == threadCountGuess) {
			threadCountGuess *= 2;
			threads = new Thread[threadCountGuess];
			// Note tg.enumerate(Thread[]) silently ignores any threads that
			// can't fit into the array
			threadCountActual = tg.enumerate(threads);
		}

		return threads;
	}

	static void earlyLeaseDaemonInit(Configuration config) throws IOException {
		ClassLoader cl = config.getClassLoader();
		if (cl instanceof ParentLastURLClassLoader) {
			if (log.isDebugEnabled()) {
				log.debug("Preventing DFS LeaseDaemon TCCL leak");
			}

			FileSystem fs = FileSystem.get(config);
			Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
			Path p = new Path("/tmp/shdp-lease-early-init-" + UUID.randomUUID().toString());
			// create/delete
			fs.create(p).close();
			fs.delete(p, false);
		}
	}
}