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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.mapreduce.JobRunner;

public class HBaseMain {

	public static String tableName = "table1";
	public static String targetTableName = "table2";
	public static String columnFamilyName = "cf";
	public static String qualifierName = "attr1";

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) {
		// Initialize spring hadoop application context
		ApplicationContext ctx = new ClassPathXmlApplicationContext(
				"META-INF/spring/context.xml");

		Configuration config = HBaseConfiguration.create();

		try {
			createTableAndInitData(tableName, columnFamilyName, qualifierName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		

		try {
			initTable(config, targetTableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// runHBaseMR();
		

		JobRunner runner = ctx.getBean("&runner", JobRunner.class);
		try {
			runner.runJob();
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		try {
			checkValue(targetTableName, config);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * run HBase MR job without spring hadoop
	 */
	private static void runHBaseMR() {

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

	/**
	 * init source table
	 * @param tableName
	 * @param cfName
	 * @param qualifier
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
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
					Bytes.toBytes(value + i % 7));
			table.put(p);
		}

		/*
		 * Get get = new Get(Bytes.toBytes(rowName + "2")); Result result =
		 * table.get(get); byte[] valueByte =
		 * result.getValue(Bytes.toBytes(cfName), Bytes.toBytes(qualifier));
		 * System.out.println("get value is:" + new String(valueByte));
		 * 
		 * 
		 * Scan scan = new Scan(); scan.addColumn(Bytes.toBytes(cfName),
		 * Bytes.toBytes(qualifier)); ResultScanner scanner =
		 * table.getScanner(scan); for (Result r : scanner) {
		 * System.out.println("scan row:" + new String(r.value())); }
		 */
	}

	/**
	 * init target table to store result
	 * @param config
	 * @param targetTable
	 * @throws IOException
	 */
	private static void initTable(Configuration config, String targetTable)
			throws IOException {
		HBaseAdmin admin = new HBaseAdmin(config);

		if (admin.tableExists(targetTable)) {
			admin.disableTable(targetTable);
			admin.deleteTable(targetTable);
		}

		HTableDescriptor tableDes = new HTableDescriptor(targetTable);
		HColumnDescriptor cf1 = new HColumnDescriptor(columnFamilyName);
		tableDes.addFamily(cf1);
		admin.createTable(tableDes);

	}

	/**
	 * check the value in the target table.
	 * @param targetTable
	 * @param config
	 * @throws IOException
	 */
	private static void checkValue(String targetTable, Configuration config)
			throws IOException {
		HTable table = new HTable(config, targetTable);

		Scan scanResult = new Scan();
		scanResult.addColumn(Bytes.toBytes(columnFamilyName),
				Bytes.toBytes("count"));
		ResultScanner scanner = table.getScanner(scanResult);
		for (Result r : scanner) {
			System.out.println(new String(r.getRow()) + ": "
					+ new String(r.value()));
		}
	}

}
