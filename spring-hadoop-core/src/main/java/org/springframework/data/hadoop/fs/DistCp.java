/*
 * Copyright 2011-2015 the original author or authors.
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

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.OptionsParser;
import org.springframework.data.hadoop.HadoopException;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Exposes the Hadoop command-line DistCp functionality as
 * an embeddable API. Due to the number of options available in DistCp, one can
 * either specify them in a command-like style (through one or multiple
 * {@link String}s) through {@link #copy(String...)} or specify individual
 * arguments through the rest of the methods.
 * 
 * @author Costin Leau
 * @author Thomas Risberg
 * @author Janne Valkealahti
 * 
 */
public class DistCp {

	private final static Log log = LogFactory.getLog(DistCp.class);
	
	private final Configuration configuration;
	private String user;

	/**
	 * Instantiates a new DistCp.
	 *
	 * @param configuration the hadoop configuration
	 */
	public DistCp(Configuration configuration) {
		this(configuration, null);
	}

	/**
	 * Instantiates a new DistCp.
	 *
	 * @param configuration the hadoop configuration
	 * @param user the user
	 */
	public DistCp(Configuration configuration, String user) {
		Assert.notNull(configuration, "configuration required");
		this.configuration = ConfigurationUtils.createFrom(configuration, null);
		// disable GenericOptionsParser
		this.configuration.setBoolean("mapred.used.genericoptionsparser", true);
		this.configuration.setBoolean("mapreduce.client.genericoptionsparser.used", true);

		this.user = user;
	}

	/**
	 * Enumeration for the possible attributes that can be preserved by a copy operation.
	 */
	public enum Preserve {
		REPLICATION, BLOCKSIZE, USER, GROUP, PERMISSION;

		static String toString(EnumSet<Preserve> preserve) {
			if (CollectionUtils.isEmpty(preserve)) {
				return "";
			}

			return toString(preserve.contains(REPLICATION), preserve.contains(BLOCKSIZE), preserve.contains(USER),
					preserve.contains(GROUP), preserve.contains(PERMISSION));
		}

		static String toString(Boolean preserveReplication, Boolean preserveBlockSize, Boolean preserveUser,
				Boolean preserveGroup, Boolean preservePermission) {

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

	/**
	 * Copies the given resources using the given parameters.
	 * 
	 * @param preserve preserve
	 * @param ignoreFailures ignoreFailures
	 * @param overwrite overwrite
	 * @param update update
	 * @param delete delete
	 * @param uris uris
	 */
	public void copy(EnumSet<Preserve> preserve, Boolean ignoreFailures, Boolean overwrite, Boolean update,
			Boolean delete, String... uris) {
		copy(preserve, ignoreFailures, Boolean.FALSE, null, null, overwrite, update, delete, null, null, null, uris);
	}

	/**
	 * Copies the given resources using the given parameters.
	 * 
	 * @param preserve preserve
	 * @param ignoreFailures ignoreFailures
	 * @param skipCrc skipCrc
	 * @param logDir logDir
	 * @param mappers mappers
	 * @param overwrite overwrite
	 * @param update update
	 * @param delete delete
	 * @param fileLimit fileLimit
	 * @param sizeLimit sizeLimit
	 * @param fileList fileList
	 * @param uris uris
	 */
	public void copy(EnumSet<Preserve> preserve, Boolean ignoreFailures, Boolean skipCrc, String logDir,
			Integer mappers, Boolean overwrite, Boolean update, Boolean delete, Long fileLimit, Long sizeLimit,
			String fileList, String... uris) {
		Boolean r = (preserve != null && preserve.contains(Preserve.REPLICATION));
		Boolean b = (preserve != null && preserve.contains(Preserve.BLOCKSIZE));
		Boolean u = (preserve != null && preserve.contains(Preserve.USER));
		Boolean g = (preserve != null && preserve.contains(Preserve.GROUP));
		Boolean p = (preserve != null && preserve.contains(Preserve.PERMISSION));

		copy(r, b, u, g, p, ignoreFailures, skipCrc, logDir, mappers, overwrite, update, delete, fileLimit, sizeLimit,
				fileList, uris);
	}

	/**
	 * Copies the given resources using the given parameters.
	 * 
	 * @param preserveReplication preserveReplication
	 * @param preserveBlockSize preserveBlockSize
	 * @param preserveUser preserveUser
	 * @param preserveGroup preserveGroup
	 * @param preservePermission preservePermission
	 * @param ignoreFailures ignoreFailures
	 * @param skipCrc skipCrc
	 * @param logDir logDir
	 * @param mappers mappers
	 * @param overwrite overwrite
	 * @param update update
	 * @param delete delete
	 * @param fileLimit fileLimit
	 * @param sizeLimit sizeLimit
	 * @param fileList fileList
	 * @param uris uris
	 */
	public void copy(Boolean preserveReplication, Boolean preserveBlockSize, Boolean preserveUser,
			Boolean preserveGroup, Boolean preservePermission, Boolean ignoreFailures, Boolean skipCrc, String logDir,
			Integer mappers, Boolean overwrite, Boolean update, Boolean delete, Long fileLimit, Long sizeLimit,
			String fileList, String... uris) {

		List<String> args = new ArrayList<String>();

		args.add(Preserve.toString(preserveReplication, preserveBlockSize, preserveUser, preserveGroup,
				preservePermission));

		if (Boolean.TRUE.equals(ignoreFailures)) {
			args.add("-i");
		}
		if (Boolean.TRUE.equals(skipCrc)) {
			args.add("-skipcrccheck");
		}
		if (logDir != null) {
			args.add("-log " + logDir);
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
		if (fileLimit != null) {
			log.warn("Hadoop DistCp v2 will ignore fileLimit argument");
			args.add("-filelimit " + fileLimit);
		}
		if (sizeLimit != null) {
			log.warn("Hadoop DistCp v2 will ignore sizeLimit argument");
			args.add("-sizelimit " + sizeLimit);
		}
		if (StringUtils.hasText(fileList)) {
			args.add("-f " + fileList);
		}

		CollectionUtils.mergeArrayIntoCollection(uris, args);
		copy(args.toArray(new String[args.size()]));
	}

	/**
	 * Initiate a basic copy operation, between a source and a destination using
	 * the defaults.
	 * 
	 * @param source the source
	 * @param destination the destination
	 */
	public void copy(String source, String destination) {
		copy(new String[] { source, destination });
	}

	/**
	 * Initiate a basic copy operation, between two sources and a destination
	 * using the defaults.
	 * 
	 * @param source1 the first source
	 * @param source2 the second source
	 * @param destination the destination
	 */
	public void copy(String source1, String source2, String destination) {
		copy(new String[] { source1, source2, destination });
	}

	/**
	 * Initiate a copy operation using a command-line style (arguments are
	 * specified as {@link String}s).
	 * 
	 * @param arguments the copy arguments
	 */
	public void copy(String... arguments) {
		Assert.notEmpty(arguments, "invalid number of arguments");
		// sanitize the arguments
		final List<String> parsedArguments = new ArrayList<String>();
		for (String arg : arguments) {
			parsedArguments.addAll(Arrays.asList(StringUtils.tokenizeToStringArray(arg, " ")));
		}

		try {
			if (StringUtils.hasText(user)) {
				UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
				ugi.doAs(new PrivilegedExceptionAction<Void>() {
					@Override
					public Void run() throws Exception {
						invokeCopy(configuration, parsedArguments.toArray(new String[parsedArguments.size()]));
						return null;
					}
				});
			}
			else {
				invokeCopy(configuration, parsedArguments.toArray(new String[parsedArguments.size()]));
			}
		} catch (Exception ex) {
			throw new IllegalStateException("Cannot run distCp impersonated as '" + user + "'", ex);
		}
	}
	
	private static void invokeCopy(Configuration config, String[] parsedArgs) {
		try {
			log.info("Running DistCp with arguments [" + StringUtils.arrayToCommaDelimitedString(parsedArgs) + "]");
			DistCpOptions inputOptions = OptionsParser.parse(parsedArgs);
			org.apache.hadoop.tools.DistCp distCp = new org.apache.hadoop.tools.DistCp(config, inputOptions);
			distCp.execute();
		} catch (Exception e) {
			throw new HadoopException("Error running DistCp job", e);
		}
	}

	/**
	 * Sets the user impersonation (optional) for creating this utility.
	 * Should be used when running against a Hadoop Kerberos cluster. 
	 * 
	 * @param user user/group information
	 */
	public void setUser(String user) {
		this.user = user;
	}
}