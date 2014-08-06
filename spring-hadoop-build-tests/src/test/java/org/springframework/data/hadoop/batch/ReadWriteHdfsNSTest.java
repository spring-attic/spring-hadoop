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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.junit.AbstractHadoopClusterTests;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


/**
 * Basic data workflow test writing data into HDFS, executing a basic job and then reading data out of HDFS.
 *
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = HadoopDelegatingSmartContextLoader.class, locations = { "/org/springframework/data/hadoop/batch/in-do-out-ns.xml" })
@MiniHadoopCluster
@DirtiesContext(classMode=ClassMode.AFTER_EACH_TEST_METHOD)
public class ReadWriteHdfsNSTest extends AbstractHadoopClusterTests {

	{
		TestUtils.hackHadoopStagingOnWin();
	}

	@Before
	public void before() {
		StepSynchronizationManager.release();
		StepSynchronizationManager.close();
	}

	@Autowired
	ApplicationContext ctx;

	@Test
	public void testWorkflowNS() throws Exception {

		// ctx.r

		FileSystem fs = FileSystem.get(ctx.getBean(Configuration.class));
		fs.delete(new Path("/ide-test/output/word/"), true);

		assertTrue(ctx.isPrototype("hadoop-tasklet"));

		JobsTrigger.startJobs(ctx);
	}

}
