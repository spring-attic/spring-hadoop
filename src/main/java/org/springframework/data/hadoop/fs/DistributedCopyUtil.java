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
package org.springframework.data.hadoop.fs;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCp.DuplicationException;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;


/**
 * Exposes the Hadoop command-line <a href="http://hadoop.apache.org/common/docs/current/distcp.html">distcp</a> as an embeddable API.
 * Due to the number of options available in DistCp, one can either specify them in a command-like style (through one or multiple {@link String}s)
 * through {@link #copy(Configuration, String...)} or specify individual arguments through the rest of the methods. 
 * 
 * @author Costin Leau
 */
public class DistributedCopyUtil {

	private DistributedCopyUtil() {
	};

	/**
	 * Returns an instance to this class (used mainly for accessibility from a scripting environment).
	 * 
	 * @return an instance to this class.
	 */
	public static DistributedCopyUtil getInstance() {
		return new DistributedCopyUtil();
	}

	public enum Preserve {
		REPLICATION, BLOCKSIZE, USER, GROUP, PERMISSION;

		static String toString(EnumSet<Preserve> preserve) {
			if (CollectionUtils.isEmpty(preserve)) {
				return "";
			}

			return toString(preserve.contains(REPLICATION), preserve.contains(BLOCKSIZE), preserve.contains(USER),
					preserve.contains(GROUP), preserve.contains(PERMISSION));
		}

		static String toString(Boolean preserveReplication, Boolean preserveBlockSize, Boolean preserveUser, Boolean preserveGroup, Boolean preservePermission) {

			StringBuilder sb = new StringBuilder();

			if (Boolean.TRUE.equals(preserveReplication)) {
				sb.append("r");
			}
			if (Boolean.TRUE.equals(preserveBlockSize)) {
				sb.append("b");
			}
			if (Boolean.TRUE.equals(preserveUser)) {
				sb.append("u");
			}
			if (Boolean.TRUE.equals(preserveGroup)) {
				sb.append("g");
			}
			if (Boolean.TRUE.equals(preservePermission)) {
				sb.append("p");
			}

			if (sb.length() > 0) {
				sb.insert(0, "-p");
			}

			return sb.toString();
		}
	}

	public static void copy(Configuration configuration, EnumSet<Preserve> preserve, Boolean ignoreFailures, Boolean overwrite, Boolean update, Boolean delete, String... uris) {

		copy(configuration, preserve, ignoreFailures, Boolean.FALSE, null, null, overwrite, update, delete, null, null,
				null, uris);
	}

	public static void copy(Configuration configuration, EnumSet<Preserve> preserve, Boolean ignoreFailures, Boolean skipCrc, String logDir, Integer mappers, Boolean overwrite, Boolean update, Boolean delete, Long fileLimit, Long sizeLimit, String fileList, String... uris) {

		Boolean r = (preserve != null && preserve.contains(Preserve.REPLICATION));
		Boolean b = (preserve != null && preserve.contains(Preserve.BLOCKSIZE));
		Boolean u = (preserve != null && preserve.contains(Preserve.USER));
		Boolean g = (preserve != null && preserve.contains(Preserve.GROUP));
		Boolean p = (preserve != null && preserve.contains(Preserve.PERMISSION));

		copy(configuration, r, b, u, g, p, ignoreFailures, skipCrc, logDir, mappers, overwrite, update, delete,
				fileLimit, sizeLimit, fileList, uris);
	}

	public static void copy(Configuration configuration, Boolean preserveReplication, Boolean preserveBlockSize, Boolean preserveUser, Boolean preserveGroup, Boolean preservePermission, Boolean ignoreFailures, Boolean skipCrc, String logDir, Integer mappers, Boolean overwrite, Boolean update, Boolean delete, Long fileLimit, Long sizeLimit, String fileList, String... uris) {

		List<String> args = new ArrayList<String>();

		args.add(Preserve.toString(preserveReplication, preserveBlockSize, preserveUser, preserveGroup,
				preservePermission));

		if (Boolean.TRUE.equals(ignoreFailures)) {
			args.add("-i");
		}
		if (Boolean.TRUE.equals(skipCrc)) {
			args.add("-skipcrccheck");
		}
		if (mappers != null) {
			args.add("-m " + mappers.intValue());
		}
		if (Boolean.TRUE.equals(overwrite)) {
			args.add("-overwrite");
		}
		if (Boolean.TRUE.equals(update)) {
			args.add("-update");
		}
		if (Boolean.TRUE.equals(delete)) {
			args.add("-delete");
		}
		if (logDir != null) {
			args.add("-log " + logDir);
		}

		copy(configuration, args.toArray(new String[args.size()]));
	}

	/**
	 * DistCopy using a command-line style (arguments are specified as {@link String}s).
	 * 
	 * @param configuration configuration to use
	 * @param arguments copy arguments
	 */
	public static void copy(Configuration configuration, String... arguments) {
		Assert.notEmpty(arguments, "invalid number of arguments");
		// sanitize the arguments
		List<String> parsedArguments = new ArrayList<String>();
		for (String arg : arguments) {
			parsedArguments.addAll(Arrays.asList(StringUtils.tokenizeToStringArray(arg, " ")));
		}

		invokeCopy(configuration, parsedArguments.toArray(new String[parsedArguments.size()]));
	}

	private static void invokeCopy(Configuration config, String[] parsedArgs) {
		try {
			Class<DistCp> cl = DistCp.class;
			Class<?> argClass = ClassUtils.resolveClassName("org.apache.hadoop.tools.DistCp$Arguments",
					cl.getClassLoader());
			Method m = ReflectionUtils.findMethod(DistCp.class, "copy", Configuration.class, argClass);
			ReflectionUtils.makeAccessible(m);
			Method v = ReflectionUtils.findMethod(argClass, "valueOf", String[].class, Configuration.class);
			ReflectionUtils.makeAccessible(v);

			// Arguments.valueOf()
			Object args = ReflectionUtils.invokeMethod(v, null, parsedArgs, config);
			// DistCp.copy()
			ReflectionUtils.invokeMethod(m, null, config, args);
		} catch (UndeclaredThrowableException ex) {
			Throwable throwable = ex.getUndeclaredThrowable();

			if (throwable instanceof IOException) {
				IOException ioe = ((IOException) throwable);
				if (ioe instanceof DuplicationException) {
					throw new IllegalStateException("Duplicated files found...", ioe);
				}
				if (ioe instanceof RemoteException) {
					throw new IllegalStateException("Cannot distCopy", ((RemoteException) ioe).unwrapRemoteException());
				}
			}

			throw ex;
		}
	}
}