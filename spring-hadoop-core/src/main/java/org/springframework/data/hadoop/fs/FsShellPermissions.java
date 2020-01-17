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
package org.springframework.data.hadoop.fs;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.BeanUtils;
import org.springframework.data.hadoop.HadoopException;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

/**
 * Utility class for accessing Hadoop FsShellPermissions (which is not public) 
 * without having to duplicate its code. 
 * 
 * @author Costin Leau
 */
abstract class FsShellPermissions {

	enum Op {
		CHOWN("-chown"), CHMOD("-chmod"), CHGRP("-chgrp");

		private final String cmd;

		Op(String cmd) {
			this.cmd = cmd;
		}

		public String getCmd() {
			return cmd;
		}
	}

	static <T> T[] concatAll(T[] first, T[]... rest) {
		// can add some sanity checks
		int totalLength = first.length;
		for (T[] array : rest) {
			totalLength += array.length;
		}
		T[] result = Arrays.copyOf(first, totalLength);
		int offset = first.length;
		for (T[] array : rest) {
			System.arraycopy(array, 0, result, offset, array.length);
			offset += array.length;
		}
		return result;
	}

	static void changePermissions(FileSystem fs, Configuration config, Op op, boolean recursive, String group, String... uris) {
		String[] argvs = new String[0];

		if (recursive) {
			ObjectUtils.addObjectToArray(argvs, "-R");
		}
		argvs = concatAll(argvs, new String[] { group }, uris);

		Class<?> cmd = ClassUtils.resolveClassName("org.apache.hadoop.fs.shell.Command", config.getClass().getClassLoader());
		Class<?> targetClz = null;
		switch (op) {
			case CHOWN:
				targetClz = ClassUtils.resolveClassName("org.apache.hadoop.fs.FsShellPermissions$Chown", config.getClass().getClassLoader());
				break;
			case CHGRP:
				targetClz = ClassUtils.resolveClassName("org.apache.hadoop.fs.FsShellPermissions$Chgrp", config.getClass().getClassLoader());
				break;
			case CHMOD:
				targetClz = ClassUtils.resolveClassName("org.apache.hadoop.fs.FsShellPermissions$Chmod", config.getClass().getClassLoader());
				break;
			default:
				throw new IllegalArgumentException(op + " is not a valid FsShell operation for FsShellPermissions");
		}
		Configurable target = (Configurable) BeanUtils.instantiate(targetClz);
		target.setConf(config);
		// run(String...) swallows the exceptions - re-implement it here
		//
		LinkedList<String> args = new LinkedList<String>(Arrays.asList(argvs));
		try {
			Method m = ReflectionUtils.findMethod(cmd, "processOptions", LinkedList.class);
			ReflectionUtils.makeAccessible(m);
			ReflectionUtils.invokeMethod(m, target, args);
			m = ReflectionUtils.findMethod(cmd, "processRawArguments", LinkedList.class);
			ReflectionUtils.makeAccessible(m);
			ReflectionUtils.invokeMethod(m, target, args);
		} catch (IllegalStateException ex){
			throw new HadoopException("Cannot change permissions/ownership " + ex);
		}
	}
}
