/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.hadoop.pig.poc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.context.support.GenericXmlApplicationContext;

/**
 * This test acts as a tiny sample that builds a small data pipeline for doing log analysis.
 *
 * @author Costin Leau
 */
public class LogAnalyzerTest {

	@Test
	public void executePOC() throws Exception {
		GenericXmlApplicationContext ctx = new GenericXmlApplicationContext(
				"/org/springframework/data/hadoop/poc/context.xml");

		ctx.registerShutdownHook();

		FileSystem fs = FileSystem.get(ctx.getBean(Configuration.class));
		fs.delete(new Path("/logs/output/"), true);
		fs.delete(new Path("/logs/output/stream"), true);


		Thread.sleep(1000 * 15);
		ctx.close();
	}
}
