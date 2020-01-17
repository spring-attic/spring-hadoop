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
package org.springframework.data.hadoop.hive;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.IndexAlreadyExistsException;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hive.service.ServiceException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.dao.NonTransientDataAccessResourceException;
import org.springframework.dao.PermissionDeniedDataAccessException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Utility for executing Hive scripts through a {@link HiveClient}. The main reason for this class
 * is to handle the parsing of the script content before submitting that to the {@link HiveClient}.
 * 
 * @author Costin Leau
 * @author Thomas Risberg
 */
abstract class HiveUtils {

	static DataAccessException convert(Exception ex) {
		if (ex == null) {
			return null;
		}

		if (ex instanceof RuntimeException) {
			throw (RuntimeException) ex;
		}

		// Thrift client exception
		if (ex instanceof ServiceException) {
			return convert(ex);
		}
		if (ex instanceof TException) {
			return new DataAccessResourceFailureException(ex.getMessage(), ex);
		}

		// HiveClient MetaStore Thrift API exceptions
		if (ex instanceof TBase) {
			// meta exceptions
			if (ex instanceof AlreadyExistsException || ex instanceof IndexAlreadyExistsException) {
				return new DataIntegrityViolationException(ex.toString(), ex);
			}
			if (ex instanceof ConfigValSecurityException) {
				return new PermissionDeniedDataAccessException(ex.toString(), ex);
			}
			// fallback
			return new InvalidDataAccessResourceUsageException(ex.toString(), ex);
		}
		// unknown
		return new NonTransientDataAccessResourceException("Unknown exception", ex);
	}

	private static DataAccessException convert(HiveServerException ex) {
		int err = ex.getErrorCode();
		String sqlState = ex.getSQLState();
		String cause = (ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage());

		// see https://issues.apache.org/jira/browse/HIVE-2661

		switch (err) {

		// 10 - semantic analysis
		case 10:
			return new DataRetrievalFailureException(cause, ex);
			// 11 - parse error
		case 11:
			return new BadSqlGrammarException("Hive query", "", new SQLException(cause, sqlState));
			// 12 - Internal error
		case 12:
			return new NonTransientDataAccessResourceException(cause, ex);
			// -10000 - another internal error
		case -10000:
			return new NonTransientDataAccessResourceException(cause, ex);
		}

		// look at the SQL code

		if ("08S01".equals(sqlState)) {
			// internal error
			return new NonTransientDataAccessResourceException(cause, ex);
		}
		// generic syntax error
		else if ("42000".equals(sqlState)) {
			return new BadSqlGrammarException("Hive query", "", new SQLException(cause, sqlState));
		}
		// not found/already exists
		else if ("42S02".equals(sqlState)) {
			return new InvalidDataAccessResourceUsageException(cause, ex);
		}
		// invalid argument
		else if ("21000".equals(sqlState)) {
			return new BadSqlGrammarException("Hive query", "", new SQLException(cause, sqlState));
		}

		// use the new Hive 0.10 codes
		// https://issues.apache.org/jira/browse/HIVE-3001

		// semantic analysis
		if (err >= 10000 && err <= 19999) {
			return new InvalidDataAccessResourceUsageException(cause, ex);
		}
		// non transient runtime errors
		else if (err >= 20000 && err <= 29999) {
			return new DataRetrievalFailureException(cause, ex);

		}
		// transient error - should retry
		else if (err >= 30000 && err <= 39999) {
			return new TransientDataAccessResourceException(cause, ex);
		}
		// internal/unknown errors
		else if (err >= 40000 && err <= 49999) {
			return new NonTransientDataAccessResourceException(cause, ex);
		}

		// unknown error
		return new NonTransientDataAccessResourceException(cause, ex);
	}

	static List<String> run(HiveClient hive, Iterable<HiveScript> scripts) throws Exception {
		Assert.notNull(scripts, "at least one script is required");

		List<String> results = new ArrayList<String>();
		for (HiveScript hiveScript : scripts) {
			results.addAll(run(hive, hiveScript));
		}
		return results;
	}

	static List<String> runWithConversion(HiveClient hive, Iterable<HiveScript> scripts, boolean closeHive) throws DataAccessException {
		try {
			return run(hive, scripts);
		} catch (Exception ex) {
			throw convert(ex);
		} finally {
			try {
				if (closeHive) {
					hive.shutdown();
				}
			} catch (Exception ex) {
				throw new InvalidDataAccessResourceUsageException("Error while closing client connection", ex);
			}
		}
	}

	/**
	 * Runs (or executes) the given script with the given parameters. Note that in order to support the given
	 * parameters, the utility will execute extra commands (hence the returned result will reflect that). 
	 * As these are client variables, they are bound to the hiveconf namespace. That means other scripts do not see them
	 * and they need to be accessed using the ${hiveconf:XXX} syntax.
	 * 
	 * @param hive hive client
	 * @param script script to run
	 * @return the script results
	 * @throws Exception
	 */
	private static List<String> run(HiveClient hive, HiveScript script) throws Exception {
		BufferedReader reader;
		InputStream stream;
		try {
			stream = script.getResource().getInputStream();
			reader = new BufferedReader(new InputStreamReader(stream));
		} catch (Exception ex) {
			throw new IllegalArgumentException("Cannot open script [" + script.getResource() + "]", ex);
		}

		List<String> results = new ArrayList<String>();

		Map<String, String> params = script.getArguments();
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
			// make sure to flush any command left (w/o ;)
			if (StringUtils.hasText(command)) {
				results.addAll(runCommand(hive, command));
			}
		} catch (IOException ex) {
			throw new IllegalArgumentException("Cannot read scripts", ex);
		} finally {
			IOUtils.closeStream(reader);
		}

		return results;
	}

	private static List<String> runCommand(HiveClient hive, String command) throws Exception {
		return hive.execute(command);
	}
}