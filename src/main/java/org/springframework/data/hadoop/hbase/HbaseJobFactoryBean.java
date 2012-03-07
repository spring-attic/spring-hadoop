package org.springframework.data.hadoop.hbase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.data.hadoop.mapreduce.JobFactoryBean;

public class HbaseJobFactoryBean extends JobFactoryBean {

	private static final Log log = LogFactory.getLog(HbaseJobFactoryBean.class);
			
	private String inputTable;
	private String outputTable;

	@Override
	protected void processJob(Job job) throws Exception {
		job.setInputFormatClass(TableInputFormat.class);
		Configuration conf = job.getConfiguration();
		HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
		if (this.inputTable != null) {
			log.info("input table is:" + this.inputTable);
			conf.set(TableInputFormat.INPUT_TABLE, inputTable);
		}
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
	    DataOutputStream dos = new DataOutputStream(out);
	    scan.write(dos);
		conf.set(TableInputFormat.SCAN, Base64.encodeBytes(out.toByteArray()));
		if (outputTable != null) {
			log.info("ouput table is:" + this.outputTable);
			TableMapReduceUtil.initTableReducerJob(outputTable,
					(Class<? extends TableReducer>) this.getReducer(), job);
		}

		TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.initCredentials(job);
	}

	public String getInputTable() {
		return inputTable;
	}

	public void setInputTable(String inputTable) {
		this.inputTable = inputTable;
	}

	public String getOutputTable() {
		return outputTable;
	}

	public void setOutputTable(String outputTable) {
		this.outputTable = outputTable;
	}

}
