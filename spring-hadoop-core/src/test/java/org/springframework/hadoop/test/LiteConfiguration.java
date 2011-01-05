/*
 * Copyright 2006-2011 the original author or authors.
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
package org.springframework.hadoop.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.hadoop.mapreduce.JobFactoryBean;
import org.springframework.stereotype.Component;

/**
 * @author Dave Syer
 * 
 */
@Component
public class LiteConfiguration {

	@Bean
	public FactoryBean<Job> getClassConfiguredJob() throws Exception {
		JobFactoryBean factory = new JobFactoryBean();
		factory.setMapper(getMapper());
		factory.setReducer(getReducer());
		factory.setCombiner(getReducer());
		factory.setOutputKeyClass(Text.class);
		factory.setOutputValueClass(IntWritable.class);
		factory.setInputPaths("src/test/resources/input");
		factory.setOutputPath("target/output");
		return factory;
	}

	@Bean
	public Mapper<Object, Text, Text, IntWritable> getMapper() {
		return new TokenizerMapper();
	}

	@Bean
	public Reducer<Text, IntWritable, Text, IntWritable> getReducer() {
		return new IntSumReducer();
	}

}
