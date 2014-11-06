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
package org.springframework.data.hadoop.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/org/springframework/data/hadoop/streaming/basic.xml")
public class StreamingTest {

	@Autowired
	private ApplicationContext ctx;
	@Resource(name = "ns-stream-job")
	private Job job;

	@Test
	public void testStreaming() throws Exception {
		cleanOutput(ctx);

		assertTrue(ctx.isPrototype("vanilla-stream-job"));
		Job job = ctx.getBean("vanilla-stream-job", Job.class);
		job.waitForCompletion(true);

		// equivalent
		// 	Configuration cfg = ctx.getBean(Configuration.class);
		//	StreamJob stream = new StreamJob();
		//	stream.setConf(cfg);
		//	String[] args = new String[] { "-verbose", "-input", "test", "-output", "output", "-mapper",
		//				"E:\\tools\\nix\\unxutils\\cat", "-reducer", "E:\\tools\\nix\\unxutils\\wc" };
		//	stream.run(args);
		//

	}

	@Test
	public void testStreamingNS() throws Exception {
		cleanOutput(ctx);
		Job job = ctx.getBean("ns-stream-job", Job.class);
		job.waitForCompletion(true);
	}

	private void cleanOutput(ApplicationContext ctx) throws Exception {
		FileSystem fs = FileSystem.get(ctx.getBean(Configuration.class));
		fs.copyFromLocalFile(new Path("../build.gradle"), new Path("test/"));
		fs.delete(new Path("output"), true);
	}

	@Test
	public void testJobProperties() throws Exception {
		assertNotNull(job);
		Configuration cfg = job.getConfiguration();
		assertNotNull(cfg);

		assertEquals("chasing", cfg.get("star"));
		assertEquals("captain eo", cfg.get("return"));
		assertEquals("last", cfg.get("train"));
		assertEquals("the dream", cfg.get("dancing"));
		assertEquals("in the mirror", cfg.get("tears"));
		assertEquals("eo", cfg.get("captain"));
	}
}
