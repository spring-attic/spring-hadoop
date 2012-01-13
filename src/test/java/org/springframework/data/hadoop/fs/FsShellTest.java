/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.hadoop.fs;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class FsShellTest {

	{
		TestUtils.hackHadoopStagingOnWin();
	}

	@Before
	public void init() throws Exception {
		TestUtils.writeToFS(cfg, "local/" + UUID.randomUUID() + ".txt");
	}

	@After
	public void destroy() throws Exception {
		FileSystem fs = FileSystem.get(cfg);
		fs.delete(new Path("local/"), true);
	}
	@Autowired
	FsShell shell;
	@Autowired
	Configuration cfg;

	@Test
	public void testChmod() throws Exception {
		String name = "local/" + UUID.randomUUID() + ".txt";
		TestUtils.writeToFS(cfg, name);
		shell.chmod("700", name);
	}
}
