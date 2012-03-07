package org.springframework.data.hadoop.samples.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class HBaseMR {

	public void createHBaseMRJob(String sourceTable, String targetTable)
			throws IOException, InterruptedException, ClassNotFoundException {
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);

		if (admin.tableExists(targetTable)) {
			admin.disableTable(targetTable);
			admin.deleteTable(targetTable);
		}

		HTableDescriptor tableDes = new HTableDescriptor(targetTable);
		HColumnDescriptor cf1 = new HColumnDescriptor("cf");
		tableDes.addFamily(cf1);
		admin.createTable(tableDes);

		Job job = new Job(config, "ExampleSummary");
		job.setJarByClass(HBaseMR.class);

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(
				sourceTable, // input table
				scan, CellValueCountMapper.class, Text.class,
				IntWritable.class, job);

		TableMapReduceUtil.initTableReducerJob(targetTable,
				CellValueCountReducer.class, job);
		// To put result into HDFS, comment out the following 2 lines of code
		// and comment the above line code.

		// job.setReducerClass(CellValueHDFSReducer.class);
		// FileOutputFormat.setOutputPath(job, new
		// Path("hdfs://localhost:9000/user/hadoop/hbase-output"));

		job.setNumReduceTasks(1);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}

		HTable table = new HTable(config, targetTable);

		Scan scanResult = new Scan();
		scanResult.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"));
		ResultScanner scanner = table.getScanner(scanResult);
		for (Result r : scanner) {
			System.out.println(new String(r.getRow()) + ": "
					+ new String(r.value()));
		}
	}

}
