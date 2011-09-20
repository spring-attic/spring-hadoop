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
package org.springframework.data.hadoop.test.word;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.configuration.JobFactoryBean;

/**
 * @author Dave Syer
 * 
 */
@Configuration
public class JobConfiguration {

	@Bean
	public FactoryBean<Job> classConfiguredJob(@Value("${input.path:src/test/resources/input/word}") String inputPath,
			@Value("${output.path:target/output/word}") String outputPath) throws Exception {
		JobFactoryBean factory = new JobFactoryBean();
		factory.setMapper(mapper());
		factory.setReducer(reducer());
		factory.setCombiner(reducer());
		factory.setOutputKeyClass(outputKeyType());
		factory.setOutputValueClass(outputValueType());
		factory.setInputPaths(inputPath);
		factory.setOutputPath(outputPath);
		return factory;
	}

	protected Class<IntWritable> outputValueType() {
		return IntWritable.class;
	}

	protected Class<Text> outputKeyType() {
		return Text.class;
	}

	@Bean
	public Mapper<?, ?, ?, ?> mapper() throws Exception {
		return new TokenizerMapper();
	}

	@Bean
	public Reducer<?, ?, ?, ?> reducer() throws Exception {
		return new IntSumReducer();
	}

	@Bean
	protected PropertyPlaceholderConfigurer externalizedConfiguration() {
		PropertyPlaceholderConfigurer configurer = new PropertyPlaceholderConfigurer();
		configurer.setSystemPropertiesMode(PropertyPlaceholderConfigurer.SYSTEM_PROPERTIES_MODE_OVERRIDE);
		return configurer;
	}

}
