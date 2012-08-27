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
package org.springframework.data.hadoop.hive;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.hadoop.io.IOUtils;
import org.apache.thrift.TException;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Utility for executing Hive scripts through a {@link HiveClient}. The main reason for this class
 * is to handle the parsing of the script content before submitting that to the {@link HiveClient}.
 * 
 * @author Costin Leau
 */
public abstract class HiveScriptRunner {


	/**
	 * Runs (or executes) all the given scripts (using "UTF-8" as the script encoding).
	 *
	 * @param hive Hive client
	 * @param scripts scripts to execute
	 * @return the scripts results
	 * @throws Exception
	 */
	public static List<List<String>> run(HiveClient hive, Iterable<Resource> scripts) throws Exception {
		return run(hive, scripts, "UTF-8");
	}

	/**
	 * Runs (or executes) all the given scripts using the specified encoding.
	 * 
	 * @param scripts
	 * @return the scripts results
	 * @throws Exception
	 */
	public static List<List<String>> run(HiveClient hive, Iterable<Resource> scripts, String encoding) throws Exception {
		Assert.notNull(hive, "a valid Hive instance is required");
		List<List<String>> results = new ArrayList<List<String>>();
		if (scripts != null) {
			for (Resource resource : scripts) {
				results.add(run(hive, resource, encoding));
			}
		}
		return results;
	}

	/**
	 * Runs (or executes) the given script (using "UTF-8" as the script encoding).
	 * 
	 * @param hive hive client
	 * @param script script to run 
	 * @return the script results
	 * @throws Exception
	 */
	public static List<String> run(HiveClient hive, Resource script) throws Exception {
		return run(hive, script, "UTF-8", null);
	}


	/**
	 * Runs (or executes) the given script (using "UTF-8" as the script encoding).
	 * 
	 * @param hive hive client
	 * @param script script to run 
	 * @return the script results
	 * @param params script parameters
	 * @throws Exception
	 */
	public static List<String> run(HiveClient hive, Resource script, Properties params) throws Exception {
		return run(hive, script, "UTF-8", params);
	}

	/**
	 * Runs (or executes) the given script.
	 * 
	 * @param hive hive client
	 * @param script script to run
	 * @param encoding script encoding
	 * @return the script results
	 * @throws Exception
	 */
	public static List<String> run(HiveClient hive, Resource script, String encoding) throws Exception {
		return run(hive, script, encoding, null);
	}

	/**
	 * Runs (or executes) the given script with the given parameters. Note that in order to support the given
	 * parameters, the utility will execute extra commands (hence the returned result will reflect that). 
	 * As these are client variables, they are bound to the hiveconf namespace. That means other scripts do not see them
	 * and they need to be accessed using the ${hiveconf:XXX} syntax.
	 * 
	 * 
	 * @param hive hive client
	 * @param script script to run
	 * @param encoding script encoding
	 * @param params script parameters
	 * @return the script results
	 * @throws Exception
	 */
	public static List<String> run(HiveClient hive, Resource script, String encoding, Properties params)
			throws Exception {
		InputStream stream = script.getInputStream();
		BufferedReader reader = new BufferedReader((StringUtils.hasText(encoding) ? new InputStreamReader(stream, encoding) : 
			new InputStreamReader(stream)));

		List<String> results = new ArrayList<String>();

		// process params first
		if (params != null) {
			for (String param : params.stringPropertyNames()) {
				String value = params.getProperty(param);
				results.addAll(runCommand(hive, "SET hiveconf:" + param + "=" + value));
			}
		}

		String line = null;
		try {
			String command = "";
			while ((line = reader.readLine()) != null) {
				// strip whitespace
				line = line.trim();
				// ignore comments
				if (!line.startsWith("--")) {
					int nrCmds = StringUtils.countOccurrencesOf(line, ";");
					for (String token : line.split(";")) {
						token = token.trim();
						// skip empty lines 
						if (StringUtils.hasText(token)) {
							command += token.concat(" ");
							if (nrCmds > 0) {
								results.addAll(runCommand(hive, command));
								nrCmds--;
								command = "";
							}
						}
					}
				}
			}
		} catch (Exception ex) {
			try {
				hive.clean();
			} catch (Exception exc) {
			}
			throw ex;
		} finally {
			IOUtils.closeStream(reader);
		}

		return results;
	}

	private static List<String> runCommand(HiveClient hive, String command) throws HiveServerException, TException {
		hive.execute(command);
		return hive.fetchAll();
	}
}