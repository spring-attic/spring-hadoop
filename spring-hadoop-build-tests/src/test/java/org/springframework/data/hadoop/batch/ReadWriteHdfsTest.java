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
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;

import static org.junit.Assert.assertTrue;


/**
 * Basic data workflow test writing data into HDFS, executing a basic job and then reading data out of HDFS.
 *
 * @author Costin Leau
 */
public class ReadWriteHdfsTest {

	@Before
	public void before() {
		StepSynchronizationManager.release();
		StepSynchronizationManager.close();
	}

	@Test
	public void testWorkflow() throws Exception {

		GenericXmlApplicationContext ctx = new GenericXmlApplicationContext(
				"/org/springframework/data/hadoop/batch/in-do-out.xml");

		ctx.registerShutdownHook();

		FileSystem fs = FileSystem.get(ctx.getBean(Configuration.class));
		System.out.println("FS is " + fs.getClass().getName());
		HdfsResourceLoader hrl = ctx.getBean(HdfsResourceLoader.class);
		Resource resource = hrl.getResource("/ide-test/output/word/");

		assertTrue(ctx.isPrototype("script-tasklet"));

		fs.delete(new Path(resource.getURI().toString()), true);

		JobsTrigger.startJobs(ctx);

		Path p = new Path("/ide-test/output/word/");
		Job job = (Job) ctx.getBean("mr-job");
		Configuration c = job.getConfiguration();
		FileSystem fs2 = p.getFileSystem(c);
		System.out.println("FS is " + fs2.getClass().getName());

		fs2.exists(p);

		ctx.close();
	}

	@Test
	public void testWorkflowNS() throws Exception {
		GenericXmlApplicationContext ctx = new GenericXmlApplicationContext(
				"/org/springframework/data/hadoop/batch/in-do-out-ns.xml");

		ctx.registerShutdownHook();

		FileSystem fs = FileSystem.get(ctx.getBean(Configuration.class));
		fs.delete(new Path("/ide-test/output/word/"), true);

		assertTrue(ctx.isPrototype("hadoop-tasklet"));

		JobsTrigger.startJobs(ctx);
		ctx.close();
	}

}
