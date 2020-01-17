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
package org.springframework.data.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.List;

/**
 * Central class for accessing the HBase API. Simplifies the use of HBase and helps to avoid common errors.
 * It executes core HBase workflow, leaving application code to invoke actions and extract results.
 *
 * @author Costin Leau
 * @author Shaun Elliott
 */
public class HbaseTemplate extends HbaseAccessor implements HbaseOperations {

	private boolean autoFlush = true;

	private Connection connection;

	private Admin admin;

	public HbaseTemplate() {
	}

	public HbaseTemplate(Configuration configuration) throws IOException {
		setConfiguration(configuration);
		afterPropertiesSet();
		this.connection = ConnectionFactory.createConnection(configuration);
		this.admin = this.connection.getAdmin();
	}

	@Override
	public <T> T execute(String tableName, TableCallback<T> action) throws IOException {
		Assert.notNull(action, "Callback object must not be null");
		Assert.notNull(tableName, "No table specified");

		Table table = getTable(tableName);

		try {
			T result = action.doInTable(table);
			this.admin.flush(table.getName());
			return result;
		} catch (Throwable th) {
			if (th instanceof Error) {
				throw ((Error) th);
			}
			if (th instanceof RuntimeException) {
				throw ((RuntimeException) th);
			}
			throw convertHbaseAccessException((Exception) th);
		} finally {
			table.close();
		}
	}

	private Table getTable(String tableName) throws IOException {
		TableName tableNameObj = TableName.valueOf(tableName);

		return connection.getTable(tableNameObj);
	}

	public DataAccessException convertHbaseAccessException(Exception ex) {
		return HbaseUtils.convertHbaseException(ex);
	}

	@Override
	public <T> T find(String tableName, String family, final ResultsExtractor<T> action) throws IOException {
		Scan scan = new Scan();
		scan.addFamily(family.getBytes(getCharset()));
		return find(tableName, scan, action);
	}

	@Override
	public <T> T find(String tableName, String family, String qualifier, final ResultsExtractor<T> action) throws IOException {
		Scan scan = new Scan();
		scan.addColumn(family.getBytes(getCharset()), qualifier.getBytes(getCharset()));
		return find(tableName, scan, action);
	}

	@Override
	public <T> T find(String tableName, final Scan scan, final ResultsExtractor<T> action) throws IOException {
		return execute(tableName, new TableCallback<T>() {
			@Override
			public T doInTable(Table htable) throws Throwable {
				ResultScanner scanner = htable.getScanner(scan);
				try {
					return action.extractData(scanner);
				} finally {
					scanner.close();
				}
			}
		});
	}

	@Override
	public <T> List<T> find(String tableName, String family, final RowMapper<T> action) throws IOException {
		Scan scan = new Scan();
		scan.addFamily(family.getBytes(getCharset()));
		return find(tableName, scan, action);
	}

	@Override
	public <T> List<T> find(String tableName, String family, String qualifier, final RowMapper<T> action) throws IOException {
		Scan scan = new Scan();
		scan.addColumn(family.getBytes(getCharset()), qualifier.getBytes(getCharset()));
		return find(tableName, scan, action);
	}

	@Override
	public <T> List<T> find(String tableName, final Scan scan, final RowMapper<T> action) throws IOException {
		return find(tableName, scan, new RowMapperResultsExtractor<T>(action));
	}

	@Override
	public <T> T get(String tableName, String rowName, final RowMapper<T> mapper) throws IOException {
		return get(tableName, rowName, null, null, mapper);
	}

	@Override
	public <T> T get(String tableName, String rowName, String familyName, final RowMapper<T> mapper) throws IOException {
		return get(tableName, rowName, familyName, null, mapper);
	}

	@Override
	public <T> T get(String tableName, final String rowName, final String familyName, final String qualifier, final RowMapper<T> mapper) throws IOException {
		return execute(tableName, new TableCallback<T>() {
			@Override
			public T doInTable(Table htable) throws Throwable {
				Get get = new Get(rowName.getBytes(getCharset()));
				if (familyName != null) {
					byte[] family = familyName.getBytes(getCharset());

					if (qualifier != null) {
						get.addColumn(family, qualifier.getBytes(getCharset()));
					}
					else {
						get.addFamily(family);
					}
				}
				Result result = htable.get(get);
				return mapper.mapRow(result, 0);
			}
		});
	}

	@Override
	public void put(String tableName, final String rowName, final String familyName, final String qualifier, final byte[] value) throws IOException {
		Assert.hasLength(rowName);
		Assert.hasLength(familyName);
		Assert.hasLength(qualifier);
		Assert.notNull(value);
		execute(tableName, new TableCallback<Object>() {
			@Override
			public Object doInTable(Table htable) throws Throwable {
				Put put = new Put(rowName.getBytes(getCharset())).addColumn(familyName.getBytes(getCharset()), qualifier.getBytes(getCharset()), value);
				htable.put(put);
				return null;
			}
		});
	}

	@Override
	public void delete(String tableName, final String rowName, final String familyName) throws IOException {
		delete(tableName, rowName, familyName, null);
	}
	
	@Override
	public void delete(String tableName, final String rowName, final String familyName, final String qualifier) throws IOException {
		Assert.hasLength(rowName);
		Assert.hasLength(familyName);
		execute(tableName, new TableCallback<Object>() {
			@Override
			public Object doInTable(Table htable) throws Throwable {
				Delete delete = new Delete(rowName.getBytes(getCharset()));
				byte[] family = familyName.getBytes(getCharset());

				if (qualifier != null) {
					delete.addColumn(family, qualifier.getBytes(getCharset()));
				}
				else {
					delete.addFamily(family);
				}
				
				htable.delete(delete);
				return null;
			}
		});
	}	

	/**
	 * Sets the auto flush.
	 *
	 * @param autoFlush The autoFlush to set.
	 */
	public void setAutoFlush(boolean autoFlush) {
		this.autoFlush = autoFlush;
	}
}