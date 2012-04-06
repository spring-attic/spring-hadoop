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

/**
 * Factory Bean to generate HBase based Map Reduce Jobs. It has similiar function with 
 * {@link org.springframework.data.hadoop.mapreduce.JobFactoryBean} which is used to generate
 * {@link org.apache.hadoop.mapreduce.Job}. But here the job has more properties which are HBase
 * specific. It is done by overriding <code>processJob</code>. 
 * 
 * @author Jarred Li
 * @since 1.0
 * @see org.springframework.data.hadoop..mapreduce.JobFactoryBean
 * 
 */
public class HbaseJobFactoryBean extends JobFactoryBean {

	private static final Log log = LogFactory.getLog(HbaseJobFactoryBean.class);

	private String inputTable;
	private String outputTable;
	
	private boolean cacheBlocks = false;
	private int cachingNum = 500;

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
		scan.setCaching(cachingNum);
		scan.setCacheBlocks(cacheBlocks);
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

	/**
	 * @return the cacheBlocks
	 */
	public boolean isCacheBlocks() {
		return cacheBlocks;
	}

	/**
	 * @param cacheBlocks the cacheBlocks to set
	 */
	public void setCacheBlocks(boolean cacheBlocks) {
		this.cacheBlocks = cacheBlocks;
	}

	/**
	 * @return the cachingNum
	 */
	public int getCachingNum() {
		return cachingNum;
	}

	/**
	 * @param cachingNum the cachingNum to set
	 */
	public void setCachingNum(int cachingNum) {
		this.cachingNum = cachingNum;
	}

}
