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
package org.springframework.data.hadoop.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;

/**
 * Central class for accessing the HBase API. Simplifies the use of HBase and helps to avoid common errors.
 * It executes core HBase workflow, leaving application code to invoke actions and extract results.
 *
 * @author Costin Leau
 */
public class HbaseTemplate extends HbaseAccessor implements HbaseOperations {

	private boolean autoFlush = true;

	public HbaseTemplate() {
	}

	public HbaseTemplate(Configuration configuration) {
		setConfiguration(configuration);
		afterPropertiesSet();
	}

	@Override
	public <T> T execute(String tableName, TableCallback<T> action) {
		Assert.notNull(action, "Callback object must not be null");
		Assert.notNull(tableName, "No table specified");

		HTableInterface table = getTable(tableName);

		try {
			boolean previousFlushSetting = applyFlushSetting(table);
			T result = action.doInTable(table);
			flushIfNecessary(table, previousFlushSetting);
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
			releaseTable(tableName, table);
		}
	}

	private HTableInterface getTable(String tableName) {
		return HbaseUtils.getHTable(tableName, getConfiguration(), getCharset(), getTableFactory());
	}

	private void releaseTable(String tableName, HTableInterface table) {
		HbaseUtils.releaseTable(tableName, table, getTableFactory());
	}

	@SuppressWarnings("deprecation")
	private boolean applyFlushSetting(HTableInterface table) {
		boolean autoFlush = table.isAutoFlush();
		if (table instanceof HTable) {
			((HTable) table).setAutoFlush(this.autoFlush);
		}
		return autoFlush;
	}

	@SuppressWarnings("deprecation")
	private void restoreFlushSettings(HTableInterface table, boolean oldFlush) {
		if (table instanceof HTable) {
			if (table.isAutoFlush() != oldFlush) {
				((HTable) table).setAutoFlush(oldFlush);
			}
		}
	}

	private void flushIfNecessary(HTableInterface table, boolean oldFlush) throws IOException {
		// TODO: check whether we can consider or not a table scope
		table.flushCommits();
		restoreFlushSettings(table, oldFlush);
	}

	public DataAccessException convertHbaseAccessException(Exception ex) {
		return HbaseUtils.convertHbaseException(ex);
	}

	@Override
	public <T> T find(String tableName, String family, final ResultsExtractor<T> action) {
		Scan scan = new Scan();
		scan.addFamily(family.getBytes(getCharset()));
		return find(tableName, scan, action);
	}

	@Override
	public <T> T find(String tableName, String family, String qualifier, final ResultsExtractor<T> action) {
		Scan scan = new Scan();
		scan.addColumn(family.getBytes(getCharset()), qualifier.getBytes(getCharset()));
		return find(tableName, scan, action);
	}

	@Override
	public <T> T find(String tableName, final Scan scan, final ResultsExtractor<T> action) {
		return execute(tableName, new TableCallback<T>() {
			@Override
			public T doInTable(HTableInterface htable) throws Throwable {
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
	public <T> List<T> find(String tableName, String family, final RowMapper<T> action) {
		Scan scan = new Scan();
		scan.addFamily(family.getBytes(getCharset()));
		return find(tableName, scan, action);
	}

	@Override
	public <T> List<T> find(String tableName, String family, String qualifier, final RowMapper<T> action) {
		Scan scan = new Scan();
		scan.addColumn(family.getBytes(getCharset()), qualifier.getBytes(getCharset()));
		return find(tableName, scan, action);
	}

	@Override
	public <T> List<T> find(String tableName, final Scan scan, final RowMapper<T> action) {
		return find(tableName, scan, new RowMapperResultsExtractor<T>(action));
	}

	@Override
	public <T> T get(String tableName, String rowName, final RowMapper<T> mapper) {
		return get(tableName, rowName, null, null, mapper);
	}

	@Override
	public <T> T get(String tableName, String rowName, String familyName, final RowMapper<T> mapper) {
		return get(tableName, rowName, familyName, null, mapper);
	}

	@Override
	public <T> T get(String tableName, final String rowName, final String familyName, final String qualifier, final RowMapper<T> mapper) {
		return execute(tableName, new TableCallback<T>() {
			@Override
			public T doInTable(HTableInterface htable) throws Throwable {
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

	/**
	 * Sets the auto flush.
	 *
	 * @param autoFlush The autoFlush to set.
	 */
	public void setAutoFlush(boolean autoFlush) {
		this.autoFlush = autoFlush;
	}
}