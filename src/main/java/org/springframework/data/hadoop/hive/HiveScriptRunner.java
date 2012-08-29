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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.hadoop.io.IOUtils;
import org.apache.thrift.TException;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.util.StringUtils;

/**
 * Utility for executing Hive scripts through a {@link HiveClient}. The main reason for this class
 * is to handle the parsing of the script content before submitting that to the {@link HiveClient}.
 * 
 * @author Costin Leau
 */
public abstract class HiveScriptRunner {


	/**
	 * Runs (or executes) the given script (using "UTF-8" as the script encoding).
	 * 
	 * @param hive hive client
	 * @param script script to run 
	 * @return the script results
	 * @throws Exception
	 */
	public static List<String> run(HiveClient hive, Resource script) throws DataAccessException {
		return run(hive, script, "UTF-8", (Map) null);
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
	public static List<String> run(HiveClient hive, Resource script, Properties params) throws DataAccessException {
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
	public static List<String> run(HiveClient hive, Resource script, String encoding) throws DataAccessException {
		return run(hive, script, encoding, (Map) null);
	}

	public static List<String> run(HiveClient hive, Resource script, String encoding, Properties params)
			throws DataAccessException {
		Map<String, String> p = null;
		if (params != null) {
			Set<String> props = params.stringPropertyNames();
			p = new LinkedHashMap<String, String>(props.size());
			for (String prop : props) {
				p.put(prop, params.getProperty(prop));
			}
		}

		return run(hive, script, encoding, p);
	}

	/**
	 * Runs (or executes) the given script with the given parameters. Note that in order to support the given
	 * parameters, the utility will execute extra commands (hence the returned result will reflect that). 
	 * As these are client variables, they are bound to the hiveconf namespace. That means other scripts do not see them
	 * and they need to be accessed using the ${hiveconf:XXX} syntax.
	 * 
	 * @param hive hive client
	 * @param script script to run
	 * @param encoding script encoding
	 * @param params script parameters
	 * @return the script results
	 * @throws Exception
	 */
	public static List<String> run(HiveClient hive, Resource script, String encoding, Map<String, String> params)
			throws DataAccessException {
		BufferedReader reader;
		InputStream stream;
		try {
			stream = script.getInputStream();
			reader = new BufferedReader(
					(StringUtils.hasText(encoding) ? new InputStreamReader(stream, encoding) : new InputStreamReader(
							stream)));
		} catch (Exception ex) {
			throw new IllegalArgumentException("Cannot open scripts", ex);
		}

		List<String> results = new ArrayList<String>();

		// process params first
		if (params != null) {
			for (Map.Entry<String, String> entry : params.entrySet()) {
				results.addAll(runCommand(hive, "SET hiveconf:" + entry.getKey() + "=" + entry.getValue()));
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
		} catch (IOException ex) {
			throw new IllegalArgumentException("Cannot read scripts", ex);
		} finally {
			IOUtils.closeStream(reader);
		}

		return results;
	}

	private static List<String> runCommand(HiveClient hive, String command) throws DataAccessException {
		try {
			hive.execute(command);
			return hive.fetchAll();
		} catch (Exception ex) {
			try {
				hive.clean();
			} catch (Exception exc) {
			}
			if (ex instanceof HiveServerException) {
				throw HiveExceptionTranslation.convert((HiveServerException) ex);
			}
			throw HiveExceptionTranslation.convert((TException) ex);
		}
	}
}