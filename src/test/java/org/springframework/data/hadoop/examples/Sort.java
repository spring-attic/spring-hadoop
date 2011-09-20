/*
 * Copyright 2006-2010 the original author or authors.
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

package org.springframework.data.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.data.hadoop.JobTemplate;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * @author Dave Syer
 * 
 */
public class Sort extends Configured implements Tool {

	private static final String SPRING_CONFIG_LOCATION = "classpath:/"
			+ ClassUtils.addResourcePathToPackagePath(Sort.class, "Sort-context.xml");

	private Map<String, String> options = new HashMap<String, String>();

	private Map<String, String> usage = new HashMap<String, String>();

	/**
	 * @param options the options to set
	 */
	public void setOptions(Map<String, String> options) {
		this.options = options;
	}

	/**
	 * @param usage the usage to set
	 */
	public void setUsage(Map<String, String> usage) {
		this.usage = usage;
	}

	private int printUsage() {
		StringBuilder builder = new StringBuilder("Sort ");
		for (String option : usage.keySet()) {
			String value = usage.get(option);
			value = StringUtils.hasText(value)? (option.length() > 0 ? " " : "") + "<" + value + ">" : "";
			builder.append("[" + option + value + "] ");
		}
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int getNumReduceTasks() throws IOException {
		Configuration configuration = getConf();
		@SuppressWarnings("deprecation")
		JobClient client = new JobClient(new org.apache.hadoop.mapred.JobConf(configuration));
		ClusterStatus cluster = client.getClusterStatus();
		int result = (int) (cluster.getMaxReduceTasks() * 0.9);
		String sort_reduces = configuration.get("test.sort.reduces_per_host");
		if (sort_reduces != null) {
			result = cluster.getTaskTrackers() * Integer.parseInt(sort_reduces);
		}
		return result;
	}

	public int run(String[] args) throws Exception {

		Configuration configuration = getConf();
		if (configuration == null) {
			configuration = new Configuration();
		}

		List<String> otherArgs = new ArrayList<String>();

		for (int i = 0; i < args.length; ++i) {
			try {
				if (options.containsKey(args[i])) {
					configuration.set(options.get(args[i]), args[++i]);
				}
				// TODO: add sampler (even in 0.21 samples it uses deprecated
				// old API)
				else {
					otherArgs.add(args[i]);
				}
			}
			catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
				return printUsage(); // exits
			}
		}

		if (otherArgs.size() > 0) {
			configuration.set("input.path", otherArgs.get(0));
		}
		if (otherArgs.size() > 1) {
			configuration.set("output.path", otherArgs.get(1));
		}

		JobTemplate jobTemplate = new JobTemplate();
		jobTemplate.setConfiguration(configuration);
		return jobTemplate.run(SPRING_CONFIG_LOCATION) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Sort(), args);
		System.exit(res);
	}

}
