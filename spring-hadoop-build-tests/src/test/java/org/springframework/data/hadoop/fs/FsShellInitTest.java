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
package org.springframework.data.hadoop.fs;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.hadoop.InitBeforeHadoop;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster(id="FsShellInitTest")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class FsShellInitTest {

	@Autowired
	private FileSystem fs;

	@Autowired
	private Configuration config;

	@Autowired
	private ResourceLoader rl;

	private Map<String, Boolean> map = InitBeforeHadoop.INSTANCES;

	@Test
	public void testFsDependsOn() throws Exception {
		assertNotNull(fs);
		assertTrue(map.get("fs-init"));
	}

	/**
	 * no longer valid as using minihadoopcluster
	 * 
	 * @throws Exception
	 **/
	// @Test
	public void testConfigDependsOn() throws Exception {
		assertNotNull(config);
		assertTrue(map.get("cfg-init"));
	}

	@Test
	public void testRlDependsOn() throws Exception {
		assertNotNull(rl);
		assertTrue(map.get("rl-init"));
	}
}
