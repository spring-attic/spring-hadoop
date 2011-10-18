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
package org.springframework.data.hadoop.configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.data.hadoop.JobTemplate;
import org.springframework.data.hadoop.mapreduce.AutowiringCombiner;
import org.springframework.data.hadoop.mapreduce.AutowiringGroupingComparator;
import org.springframework.data.hadoop.mapreduce.AutowiringInputFormat;
import org.springframework.data.hadoop.mapreduce.AutowiringMapper;
import org.springframework.data.hadoop.mapreduce.AutowiringOutputFormat;
import org.springframework.data.hadoop.mapreduce.AutowiringPartitioner;
import org.springframework.data.hadoop.mapreduce.AutowiringReducer;
import org.springframework.data.hadoop.mapreduce.AutowiringSortComparator;
import org.springframework.util.ObjectUtils;

/**
 * A factory bean for a {@link Job} to be configured with Spring and used as a
 * template to launch Hadoop processes. Instead of configuring like a native job
 * with class names for the mapper, reducer etc, this factory allows you to
 * inject concrete instances of the dependencies. Those instances will be used
 * as is for a local job or re-created from a separate application context
 * remotely for a clustered job.
 * 
 * @see JobTemplate for template usage patterns
 * 
 * @author Dave Syer
 * @author Costin Leau
 */
public class AutowiredJobFactoryBean extends JobFactoryBean {

	protected void processJob(Job job) throws Exception {
		Path[] iPaths = FileInputFormat.getInputPaths(job);

		if (!ObjectUtils.isEmpty(iPaths)) {
			String[] iPathsString = new String[iPaths.length];

			for (int i = 0; i < iPaths.length; i++) {
				iPathsString[0] = iPaths[0].toString();
			}

			job.getConfiguration().setStrings(JobTemplate.SPRING_INPUT_PATHS, iPathsString);
		}


		Path oPath = FileOutputFormat.getOutputPath(job);
		if (oPath != null) {
			job.getConfiguration().set(JobTemplate.SPRING_OUTPUT_PATH, oPath.toString());
		}

		if (job.getMapperClass() != null) {
			job.setMapperClass(AutowiringMapper.class);
		}
		if (job.getCombinerClass() != null) {
			job.setCombinerClass(AutowiringCombiner.class);
		}
		if (job.getReducerClass() != null) {
			job.setReducerClass(AutowiringReducer.class);
		}
		if (job.getInputFormatClass() != null) {
			job.setInputFormatClass(AutowiringInputFormat.class);
		}
		if (job.getOutputFormatClass() != null) {
			job.setOutputFormatClass(AutowiringOutputFormat.class);
		}
		if (job.getSortComparator() != null) {
			job.setSortComparatorClass(AutowiringSortComparator.class);
		}
		if (job.getGroupingComparator() != null) {
			job.setGroupingComparatorClass(AutowiringGroupingComparator.class);
		}
		if (job.getPartitionerClass() != null) {
			job.setPartitionerClass(AutowiringPartitioner.class);
		}
	}
}