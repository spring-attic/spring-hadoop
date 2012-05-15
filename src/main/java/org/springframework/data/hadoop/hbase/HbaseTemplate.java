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
package org.springframework.data.hadoop.hbase;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;

/**
 * Central class for accessing the HBase API. Simplifies the use of HBase and helps to avoid common errors. 
 * It executes core HBase workflow, leaving application code to invoke actions and extract results. 
 * 
 * @author Costin Leau
 */
public class HbaseTemplate implements InitializingBean {

	private Charset charset;
	private String encoding;
	private String defaultTable;
	private boolean autoFlush = true;
	private HTableInterfaceFactory tableFactory;
	private Configuration configuration;

	public HbaseTemplate() {
	}

	public HbaseTemplate(Configuration configuration) {
		this.configuration = configuration;
		afterPropertiesSet();
	}

	@Override
	public void afterPropertiesSet() {
		Assert.notNull(configuration, "configuration is required");

		// detect charset
		charset = HbaseUtils.getCharset(encoding);
	}

	public <T> T execute(TableCallback<T> action) {
		return execute(defaultTable, action);
	}

	/**
	 * Executes the given action against the specified table handling resource management.
	 * <p/>
	 * Application exceptions thrown by the action object get propagated to the caller (can only be unchecked). Allows for returning a 
	 * result object (typically a domain object or collection of domain objects).
	 * 
	 * @param tableName
	 * @param tableCallback
	 * @return
	 */
	public <T> T execute(String tableName, TableCallback<T> action) {
		Assert.notNull(action, "Callback object must not be null");
		Assert.notNull(tableName, "No table specified");

		HTable table = getTable(tableName);

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
			try {
				closeTable(table);
			} catch (IOException ex) {
				throw convertHbaseAccessException(ex);
			} catch (IllegalStateException e) {
				throw convertHbaseAccessException(e);
			}
		}
	}

	private HTable getTable(String tableName) {
		return HbaseUtils.getHTable(tableFactory, charset, configuration, tableName);
	}
	
	private void closeTable(HTable table) throws IOException, IllegalStateException {
		HbaseUtils.closeHTable(table);
	}

	private boolean applyFlushSetting(HTable table) {
		boolean autoFlush = table.isAutoFlush();
		table.setAutoFlush(this.autoFlush);
		return autoFlush;
	}

	private void flushIfNecessary(HTable table, boolean oldFlush) throws IOException {
		// TODO: check whether we can consider or not a table scope
		table.flushCommits();
		if (table.isAutoFlush() != oldFlush) {
			table.setAutoFlush(oldFlush);
		}
	}

	public DataAccessException convertHbaseAccessException(Exception ex) {
		return HbaseUtils.convertHbaseException(ex);
	}

	public <T> T execute(String tableName, String family, final ResultsExtractor<T> action) {
		Scan scan = new Scan();
		scan.addFamily(family.getBytes(charset));
		return execute(tableName, scan, action);
	}

	public <T> T execute(String tableName, String family, String qualifier, final ResultsExtractor<T> action) {
		Scan scan = new Scan();
		scan.addColumn(family.getBytes(charset), qualifier.getBytes(charset));
		return execute(tableName, scan, action);
	}

	public <T> T execute(String tableName, final Scan scan, final ResultsExtractor<T> action) {
		return execute(tableName, new TableCallback<T>() {
			@Override
			public T doInTable(HTable htable) throws Throwable {
				ResultScanner scanner = htable.getScanner(scan);
				try {
					return action.extractData(scanner);
				} finally {
					scanner.close();
				}
			}
		});
	}

	public <T> List<T> find(String tableName, String family, final RowMapper<T> action) {
		Scan scan = new Scan();
		scan.addFamily(family.getBytes(charset));
		return find(tableName, scan, action);
	}

	public <T> List<T> find(String tableName, String family, String qualifier, final RowMapper<T> action) {
		Scan scan = new Scan();
		scan.addColumn(family.getBytes(charset), qualifier.getBytes(charset));
		return find(tableName, scan, action);
	}

	public <T> List<T> find(String tableName, final Scan scan, final RowMapper<T> action) {
		return execute(tableName, scan, new RowMapperResultsExtractor<T>(action));
	}

	public <T> T get(String tableName, String rowName, final RowMapper<T> mapper) {
		return get(tableName, rowName, null, null, mapper);
	}

	public <T> T get(String tableName, String rowName, String familyName, final RowMapper<T> mapper) {
		return get(tableName, rowName, familyName, null, mapper);
	}

	public <T> T get(String tableName, final String rowName, final String familyName, final String qualifier, final RowMapper<T> mapper) {
		return execute(tableName, new TableCallback<T>() {
			@Override
			public T doInTable(HTable htable) throws Throwable {
				Get get = new Get(rowName.getBytes(charset));
				if (familyName != null) {
					byte[] family = familyName.getBytes(charset);

					if (qualifier != null) {
						get.addColumn(family, qualifier.getBytes(charset));
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
	 * @param autoFlush The autoFlush to set.
	 */
	public void setAutoFlush(boolean autoFlush) {
		this.autoFlush = autoFlush;
	}


	/**
	 * @param tableFactory The tableFactory to set.
	 */
	public void setTableFactory(HTableInterfaceFactory tableFactory) {
		this.tableFactory = tableFactory;
	}

	/**
	 * @param encoding The encoding to set.
	 */
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	/**
	 * @param defaultTable The defaultTable to set.
	 */
	public void setDefaultTable(String defaultTable) {
		this.defaultTable = defaultTable;
	}

	/**
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}
}