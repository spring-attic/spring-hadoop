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

import java.sql.SQLException;

import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.thrift.TException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.dao.NonTransientDataAccessResourceException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.BadSqlGrammarException;

/**
 * Utility class converting Hive exceptions to DataAccessExceptions. 
 * 
 * @author Costin Leau
 */
abstract class HiveExceptionTranslation {

	static DataAccessException convert(HiveServerException ex) {
		if (ex == null) {
			return null;
		}

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

	static DataAccessException convert(TException ex) {
		if (ex == null) {
			return null;
		}

		return new DataAccessResourceFailureException(ex.getMessage(), ex);
	}
}