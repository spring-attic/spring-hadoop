/*
 * Copyright 2011 the original author or authors.
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

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

/**
 * Test for basic HBase connectivity (based on the API usage in client package).
 *
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class BasicHBaseTest {

	@Resource(name = "hbaseConfiguration")
	Configuration config;

	@Autowired
	HbaseTemplate template;

	String tableName = "myTable";
	String columnName = "myColumnFamily";
	String rowName = "myLittleRow";
	String qualifier = "someQualifier";
	String value = "Some Value";

	@SuppressWarnings({ "deprecation", "resource" })
	@Test
	public void testHBaseConnection() throws Exception {
		HBaseAdmin admin = new HBaseAdmin(config);
		if (admin.tableExists(tableName)) {
			System.out.println("deleting table...");
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}

		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		tableDescriptor.addFamily(new HColumnDescriptor(columnName));
		assertTrue(tableDescriptor.hasFamily(Bytes.toBytes(columnName)));
		admin.createTable(tableDescriptor);

		// block A
		System.out.println("Created table...");
		HTable table = new HTable(config, tableName);

		Put p = new Put(Bytes.toBytes(rowName));
		p.add(Bytes.toBytes(columnName), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		table.put(p);

		System.out.println("Doing put..");
		Get g = new Get(Bytes.toBytes(rowName));
		Result r = table.get(g);
		byte[] val = r.getValue(Bytes.toBytes(columnName), Bytes.toBytes(qualifier));

		assertEquals(value, Bytes.toString(val));
		System.out.println("Doing get..");


		// block B
		Scan s = new Scan();
		s.addColumn(Bytes.toBytes(columnName), Bytes.toBytes(qualifier));
		ResultScanner scanner = table.getScanner(s);

		try {
			// Scanners return Result instances.
			for (Result rr : scanner) {
				System.out.println("Found row: " + rr);
			}
		} catch (Exception ex) {
			System.out.println("Caught exception " + ex);
		} finally {
			// Make sure you close your scanners when you are done!
			// Thats why we have it inside a try/finally clause
			scanner.close();
		}
	}

	@Test
	public void testTemplate() throws Exception {
		assertTrue(HbaseSynchronizationManager.getTableNames().isEmpty());

		// manual bind
		final HTableInterface t = HbaseUtils.getHTable(tableName, config);
		HbaseSynchronizationManager.bindResource(tableName, t);

		template.execute(tableName, new TableCallback<Object>() {
			@Override
			public Object doInTable(HTableInterface table) throws Throwable {
				Put p = new Put(Bytes.toBytes(rowName));
				p.add(Bytes.toBytes(columnName), Bytes.toBytes(qualifier), Bytes.toBytes(value));
				table.put(p);

				System.out.println("Doing put..");
				Get g = new Get(Bytes.toBytes(rowName));
				Result r = table.get(g);
				byte[] val = r.getValue(Bytes.toBytes(columnName), Bytes.toBytes(qualifier));

				assertEquals(value, Bytes.toString(val));

				assertSame(t, table);
				return null;
			}
		});

		assertFalse(HbaseSynchronizationManager.getTableNames().isEmpty());
		assertTrue(HbaseSynchronizationManager.hasResource(tableName));

		// equivalent of block B
		System.out.println("Found rows " + template.find(tableName, columnName, qualifier, new RowMapper<String>() {
			@Override
			public String mapRow(Result result, int rowNum) throws Exception {
				return result.toString();
			}
		}));

		// manual unbind
		HbaseSynchronizationManager.unbindResource(tableName);
		HbaseUtils.releaseTable(tableName, t);
		assertTrue(HbaseSynchronizationManager.getTableNames().isEmpty());
	}
}