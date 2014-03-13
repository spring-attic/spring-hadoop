/*
 * Copyright 2011-2013 the original author or authors.
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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Costin Leau
 */
@SuppressWarnings("rawtypes")
public class Mapper extends org.apache.hadoop.mapreduce.Mapper {

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		if (!"bucket".equals(configuration.get("head"))) {
			throw new IllegalStateException("incorrect config given");
		}
		System.out.println(configuration);
	}
}
