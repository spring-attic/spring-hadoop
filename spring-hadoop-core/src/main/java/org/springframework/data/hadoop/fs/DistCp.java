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
package org.springframework.data.hadoop.fs;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.OptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Exposes the Hadoop command-line distcp as
 * an embeddable API.
 * 
 * @author liujiong
 */
public class DistCp {

	private static Configuration configuration = null;
	private String user;

	/**
	 * Constructs a new <code>DistCp</code> instance.
	 *
	 * @param configuration
	 *            Hadoop configuration to use.
	 */
	public DistCp(Configuration configuration) {
		this(configuration, null);
	};

	public DistCp(Configuration configuration, String user) {
		Assert.notNull(configuration, "configuration required");
		this.configuration = ConfigurationUtils.createFrom(configuration, null);
		// disable GenericOptionsParser
//		this.configuration.setBoolean("mapred.used.genericoptionsparser", true);
//		this.configuration.setBoolean(
//				"mapreduce.client.genericoptionsparser.used", true);

		this.user = user;
	}


	/**
	 * DistCopy using a command-line style (arguments are specified as
	 * {@link String}s).
	 * 
	 * @param arguments
	 *            copy arguments
	 */
	public void copy(String... arguments) {
		Assert.notEmpty(arguments, "invalid number of arguments");
		// sanitize the arguments
		final List<String> parsedArguments = new ArrayList<String>();
		for (String arg : arguments) {
			parsedArguments.addAll(Arrays.asList(StringUtils
					.tokenizeToStringArray(arg, " ")));
		}

		try {
			if (StringUtils.hasText(user)) {
				UserGroupInformation ugi = UserGroupInformation
						.createProxyUser(user,
								UserGroupInformation.getLoginUser());
				ugi.doAs(new PrivilegedExceptionAction<Void>() {
					@Override
					public Void run() throws Exception {
						invokeCopy(configuration, parsedArguments
								.toArray(new String[parsedArguments.size()]));
						return null;
					}
				});
			} else {
				invokeCopy(configuration,
						parsedArguments.toArray(new String[parsedArguments
								.size()]));
			}
		} catch (Exception ex) {
			throw new IllegalStateException(
					"Cannot run distCp impersonated as '" + user + "'", ex);
		}
	}

	private static void invokeCopy(Configuration config, String[] parsedArgs)
			throws Exception {
		org.apache.hadoop.tools.DistCp distCp = new org.apache.hadoop.tools.DistCp(configuration,
				OptionsParser.parse(parsedArgs));
		ToolRunner.run(configuration, distCp, parsedArgs);

	}

	/**
	 * Sets the user impersonation (optional) for creating this utility. Should
	 * be used when running against a Hadoop Kerberos cluster.
	 * 
	 * @param user
	 *            user/group information
	 */
	public void setUser(String user) {
		this.user = user;
	}
}