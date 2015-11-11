/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.hadoop.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.hadoop.mapreduce.JobRunner;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Simple tests checking mini cluster integration.
 * 
 * @author Janne Valkealahti
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@Ignore
public class HadoopClusterTests {

	protected Log log = LogFactory.getLog(getClass());

	@Autowired
	private ApplicationContext ctx;
	
	@Test
	public void testConfiguredConfigurationWithJobRun() throws Exception {
		assertTrue(ctx.containsBean("hadoopConfiguration"));
		Configuration config = (Configuration) ctx.getBean("hadoopConfiguration");
		assertNotNull(config);

        Path inDir = new Path("testing/jobconf/input");
        Path outDir = new Path("testing/jobconf/output");
		FileSystem fs = FileSystem.get(config);
		fs.delete(inDir, true);
		fs.delete(outDir, true);
		fs.mkdirs(inDir);

        OutputStream os = fs.create(new Path(inDir, "text.txt"));
        Writer wr = new OutputStreamWriter(os);
        wr.write("b a\n");
        wr.close();

        Job job = ctx.getBean("wordcountJob", Job.class);
		
		try {
			JobRunner runner = (JobRunner) ctx.getBean("runner");
			runner.call();

			long end = System.currentTimeMillis() + 180000;
			do {
				boolean isComplete = job.isComplete();
				log.info("Job isComplete=" + isComplete);
				if (job.isComplete()) {
					break;
				}
				Thread.sleep(1000);
			} while (System.currentTimeMillis() < end);
			assertTrue(job.isComplete());
			assertTrue(job.isSuccessful());

			Path[] outputFiles = FileUtil.stat2Paths(
			        fs.listStatus(outDir, new Utils.OutputFileUtils.OutputFilesFilter()));

			assertEquals(1, outputFiles.length);

			InputStream in = fs.open(outputFiles[0]);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			assertEquals("foo\t1", reader.readLine());
			assertNull(reader.readLine());
			reader.close();
		} catch (Exception e) {
			// attempt to copy minicluster files before
			// those are cleaned
			try {
				FileUtils.copyDirectory(new File("target/HadoopClusterTests"),
						new File("target/HadoopClusterTests-copy"));
			} catch (Exception e2) {
				log.error("Can't make copy of target/HadoopClusterTests", e2);
			}
			// printing info before failing test
			try {
				log.info("Job info: " + job);
			} catch (Exception e1) {
				log.info("Can't print job info", e1);
			}
			throw e;
		}
	}	
	
}
