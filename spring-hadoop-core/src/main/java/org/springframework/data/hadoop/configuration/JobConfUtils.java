/*
 * Copyright 2011-2014 the original author or authors.
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.util.Map;
import java.util.Properties;

/**
 * Reusable utility class for common {@link org.apache.hadoop.mapred.JobConf} operations.
 *
 * @author Thomas Risberg
 */
public abstract class JobConfUtils {

	/**
	 * Creates a new {@link org.apache.hadoop.mapred.JobConf} based on the given arguments.
	 *
	 * @param original initial configuration to read from. May be null.
	 * @param properties properties object to add to the newly created configuration. May be null.
	 * @return newly created configuration based on the input parameters.
	 */
	public static JobConf createFrom(Configuration original, Properties properties) {
		JobConf cfg = null;
		if (original != null) {
			cfg = new JobConf(original);
		}
		else {
			cfg = new JobConf();
		}
		ConfigurationUtils.addProperties(cfg, properties);
		return cfg;
	}

	/**
	 * Creates a new {@link org.apache.hadoop.conf.Configuration} by merging the given configurations.
	 * Ordering is important - the second configuration overriding values in the first.
	 * 
	 * @param one configuration to read from. May be null.
	 * @param two configuration to read from. May be null.
	 * @return the result of merging the two configurations.
	 */
	public static JobConf merge(Configuration one, Configuration two) {
		if (one == null) {
			if (two == null) {
				return new JobConf();
			}
			return new JobConf(two);
		}

		JobConf c = new JobConf(one);

		if (two == null) {
			return c;
		}

		for (Map.Entry<String, String> entry : two) {
			c.set(entry.getKey(), entry.getValue());
		}

		return c;
	}

}