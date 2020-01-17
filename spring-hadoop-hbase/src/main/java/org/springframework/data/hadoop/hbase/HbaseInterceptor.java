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

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

//import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * AOP interceptor that binds a new Hbase table to the thread before a method call, closing and removing it afterwards in case of any method outcome.
 * If there is already a pre-bound table (from a previous call or transaction), the interceptor simply participates in it.
 * Typically used alongside {@link HbaseSynchronizationManager}.
 *
 * @author Costin Leau
 */
public class HbaseInterceptor extends HbaseAccessor implements MethodInterceptor {

	private static final Log log = LogFactory.getLog(HbaseInterceptor.class);

	private boolean exceptionConversionEnabled = true;
	private String[] tableNames;


	@Override
	public void afterPropertiesSet() {
		super.afterPropertiesSet();
		Assert.notEmpty(tableNames, "at least one table needs to be specified");
	}

	@Override
	public Object invoke(MethodInvocation methodInvocation) throws Throwable {
		Set<String> boundTables = new LinkedHashSet<String>();

		for (String tableName : tableNames) {
			if (!HbaseSynchronizationManager.hasResource(tableName)) {
				boundTables.add(tableName);

				TableName tableNameObj = TableName.valueOf(tableName);

				Connection connection = ConnectionFactory.createConnection(getConfiguration());
				Table table = connection.getTable(tableNameObj);

				HbaseSynchronizationManager.bindResource(tableName, table);
			}
		}

		try {
			Object retVal = methodInvocation.proceed();
			return retVal;
		} catch (Exception ex) {
			if (this.exceptionConversionEnabled) {
				throw convertHBaseException(ex);
			}
			else {
				throw ex;
			}
		} finally {
			for (String tableName : boundTables) {
				Table table =  HbaseSynchronizationManager.unbindResourceIfPossible(tableName);
				if (table != null) {
					table.close();
				}
				else {
					log.warn("Table [" + tableName + "] unbound from the thread by somebody else; cannot guarantee proper clean-up");
				}
			}
		}
	}

	private Exception convertHBaseException(Exception ex) {
		return HbaseUtils.convertHbaseException(ex);
	}

	public void setTableNames(String[] tableNames) {
		this.tableNames = tableNames;
	}

	/**
	 * Sets whether to convert any {@link IOException} raised to a Spring DataAccessException,
	 * compatible with the <code>org.springframework.dao</code> exception hierarchy.
	 * <p>Default is "true". Turn this flag off to let the caller receive raw exceptions
	 * as-is, without any wrapping.
	 * @see org.springframework.dao.DataAccessException
	 *
	 * @param exceptionConversionEnabled enable exceptionConversion
	 */
	public void setExceptionConversionEnabled(boolean exceptionConversionEnabled) {
		this.exceptionConversionEnabled = exceptionConversionEnabled;
	}
}