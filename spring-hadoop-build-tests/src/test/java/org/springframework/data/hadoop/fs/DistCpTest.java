/*
 * Copyright 2011-2015 the original author or authors.
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

import java.util.EnumSet;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Tests for {@link DistCp}.
 * 
 * @author Costin Leau
 * @author Janne Valkealahti
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class DistCpTest {

	@Autowired
	Configuration cfg;

	private static final String dir = "distcp/";

	@After
	public void destroy() throws Exception {
		FileSystem fs = FileSystem.get(cfg);
		fs.delete(new Path(dir), true);
	}

	@Test
	public void testBasicCopy() throws Exception {
		String src = dir + UUID.randomUUID();
		TestUtils.writeToFS(cfg, src);

		HdfsResourceLoader loader = new HdfsResourceLoader(cfg);

		String srcA = loader.getResource(src).getURI().toString();
		String dstA = loader.getResource(dir + "dst/").getURI().toString();

		loader.close();

		System.out.println(srcA);
		System.out.println(dstA);

		new DistCp(cfg).copy(srcA, dstA);
	}

	@Test
	public void testMultipleCopy() throws Exception {
		String src1 = dir + UUID.randomUUID();
		String src2 = dir + UUID.randomUUID();

		TestUtils.writeToFS(cfg, src1);
		TestUtils.writeToFS(cfg, src2);

		HdfsResourceLoader loader = new HdfsResourceLoader(cfg);
		String src1A = loader.getResource(src1).getURI().toString();
		String src2A = loader.getResource(src2).getURI().toString();
		String dstA = loader.getResource(dir + "dst/").getURI().toString();

		loader.close();
		new DistCp(cfg).copy(src1A, src2A, dstA);
	}

	@Test
	public void testCopyPreserve() throws Exception {
		String src = dir + UUID.randomUUID();
		TestUtils.writeToFS(cfg, src);

		HdfsResourceLoader loader = new HdfsResourceLoader(cfg);

		String srcA = loader.getResource(src).getURI().toString();
		String dstA = loader.getResource(dir + "dst/").getURI().toString();
		loader.close();

		System.out.println(srcA);
		System.out.println(dstA);

		new DistCp(cfg).copy(EnumSet.allOf(DistCp.Preserve.class), false, true, false, false, srcA, dstA);
	}

}