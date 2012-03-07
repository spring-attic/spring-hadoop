package org.springframework.data.hadoop.samples.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class HBaseMain {

	private static String tableName = "table1";
	private static String targetTableName = "table2";
	private static String columnFamilyName = "cf";
	private static String qualifierName = "attr1";

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) {
		// Initialize spring hadoop application context
		new ClassPathXmlApplicationContext("META-INF/spring/context.xml");

		try {
			createTableAndInitData(tableName, columnFamilyName, qualifierName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		runHBaseMR();
	}

	public static void runHBaseMR() {

		HBaseMR mr = new HBaseMR();
		try {
			mr.createHBaseMRJob(tableName, targetTableName);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static void createTableAndInitData(String tableName, String cfName,
			String qualifier) throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {

		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);

		String rowName = "row";
		String value = "value";

		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}

		HTableDescriptor tableDes = new HTableDescriptor(tableName);
		HColumnDescriptor cf1 = new HColumnDescriptor(cfName);
		tableDes.addFamily(cf1);
		admin.createTable(tableDes);

		HTable table = new HTable(config, tableName);

		for (int i = 0; i < 1000; i++) {
			Put p = new Put(Bytes.toBytes(rowName + i));
			p.add(Bytes.toBytes(cfName), Bytes.toBytes(qualifier),
					Bytes.toBytes(value + i%7));
			table.put(p);
		}

		/*
		Get get = new Get(Bytes.toBytes(rowName + "2"));
		Result result = table.get(get);
		byte[] valueByte = result.getValue(Bytes.toBytes(cfName),
				Bytes.toBytes(qualifier));
		System.out.println("get value is:" + new String(valueByte));

		
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier));
		ResultScanner scanner = table.getScanner(scan);
		for (Result r : scanner) {
			System.out.println("scan row:" + new String(r.value()));
		}
		*/
	}
}
