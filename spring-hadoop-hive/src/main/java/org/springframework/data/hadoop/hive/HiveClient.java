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

import org.springframework.dao.DataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Client class that replaces the HiveClient from Hiveserver1. It helps simplify Hive data access code.
 * Automatically handles the creation of a {@link java.sql.Connection} to Hive.
 *
 * This class is not thread safe. It uses a JdbcTemplate constructed from a DataSource that is not necessarily
 * thread safe.
 *
 * @author Thomas Risberg
 */
public class HiveClient {

	private JdbcTemplate jdbcTemplate;

	public HiveClient(DataSource dataSource) {
		jdbcTemplate = new JdbcTemplate(dataSource);
	}

	public Connection getConnection() {
		return DataSourceUtils.getConnection(jdbcTemplate.getDataSource());
	}

	public String executeAndfetchOne(final String command) {
		List<String> results = execute(command);
		if (results.size() < 1) {
			throw new IncorrectResultSizeDataAccessException(1);
		}
		return results.get(0);
	}

	public List<String> execute(final String command) {
		return jdbcTemplate.execute(new ConnectionCallback<List<String>>() {
			@Override
			public List<String> doInConnection(Connection con) throws SQLException, DataAccessException {
				List<String> results = new ArrayList<String>();
				Statement stmt = con.createStatement();
				ResultSet rs = null;
				int i = 0;
				try {
					boolean retRs = stmt.execute(command);
					if (retRs) {
						rs = stmt.getResultSet();
						while (rs.next()) {
							results.add(rs.getString(1));
							i++;
						}
					}
				}
				finally {
					if (rs != null) {
						try {
							rs.close();
						}
						catch (Exception ignore) {}
					}
					if (stmt != null) {
						try {
							stmt.close();
						}
						catch (Exception ignore) {}
					}
				}
				return results;
			}
		});
	}

	JdbcOperations getJdbcOperations() {
		return jdbcTemplate;
	}

	public void shutdown() throws SQLException {
		jdbcTemplate.getDataSource().getConnection().close();
	}
}
