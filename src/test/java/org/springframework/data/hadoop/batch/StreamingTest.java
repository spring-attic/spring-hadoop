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
package org.springframework.data.hadoop.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.streaming.StreamJob;
import org.junit.Test;
import org.springframework.context.support.GenericXmlApplicationContext;

/**
 * Test running a basic streaming example
 * 
 * $HADOOP_HOME/bin/hadoop  jar $HADOOP_HOME/hadoop-streaming.jar \
 *    -D mapreduce.job.reduces=2 \
 *   -input myInputDirs \
 *   -output myOutputDir \
 *   -mapper /bin/cat \
 *   -reducer /bin/wc 
 *
 * 
 * @author Costin Leau
 */
public class StreamingTest {

	@Test
	public void testStreaming() throws Exception {
		GenericXmlApplicationContext ctx = new GenericXmlApplicationContext(
				"/org/springframework/data/hadoop/streaming/basic.xml");

		ctx.registerShutdownHook();


		FileSystem fs = ctx.getBean(FileSystem.class);
		fs.copyFromLocalFile(new Path("./build.gradle"), new Path("test/"));
		fs.delete(new Path("output"), true);

		Configuration cfg = ctx.getBean(Configuration.class);
		StreamJob stream = new StreamJob();
		stream.setConf(cfg);


		String[] args = new String[] { "-verbose", "-input", "test", "-output", "output", "-mapper",
				"E:\\tools\\nix\\unxutils\\cat", "-reducer", "E:\\tools\\nix\\unxutils\\wc" };
		stream.run(args);

	}
}
