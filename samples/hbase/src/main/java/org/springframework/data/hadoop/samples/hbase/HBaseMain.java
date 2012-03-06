package org.springframework.data.hadoop.samples.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class HBaseMain {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		
		AbstractApplicationContext ctx = new ClassPathXmlApplicationContext("META-INF/spring/context.xml");
		
		
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);
		String tableName = "table1";
		String familyName = "family1";
		String qualifierName = "c1";
		String rowName = "row1";
		String value = "value1";

		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		HTableDescriptor tableDes = new HTableDescriptor(tableName);
		HColumnDescriptor cf1 = new HColumnDescriptor(familyName);
		tableDes.addFamily(cf1);
		admin.createTable(tableDes);
		
		HTable table = new HTable(config,tableName); 
		
		
		Put p = new Put(Bytes.toBytes(rowName));
		p.add(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName), Bytes.toBytes(value));		
		table.put(p);
		
		Get get = new Get(Bytes.toBytes(rowName));
		Result result = table.get(get);
		byte[] valueByte = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName));		
		System.out.println("get value is:" + new String(valueByte));
		
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName));
		ResultScanner scanner = table.getScanner(scan);
		for(Result r : scanner){
			System.out.println("scan row:" + new String(r.value()));
		}
	}
}
