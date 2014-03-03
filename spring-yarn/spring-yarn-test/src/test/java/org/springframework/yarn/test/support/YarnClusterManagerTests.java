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
package org.springframework.yarn.test.support;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;
import org.springframework.yarn.test.YarnTestSystemConstants;
import org.springframework.yarn.test.context.YarnCluster;

/**
 * Raw cluster tests through {@link YarnClusterManager}.
 * 
 * @author Janne Valkealahti
 *
 */
public class YarnClusterManagerTests {
	
	private static String BASE = YarnTestSystemConstants.HDFS_TESTS_BASE_PATH + "YarnClusterManagerTests/";
	private static String path1 = BASE + "file1";
	private static String path2 = BASE + "file2";
	private static String path3 = BASE + "file3";

	@Test
	public void testOneStandalone() throws Exception {
		YarnClusterManager manager = YarnClusterManager.getInstance();
		YarnCluster cluster = manager.getCluster(new ClusterInfo());
		cluster.start();
		
		Configuration configuration = cluster.getConfiguration();
		
		// these are not yet created
		checkFileNotExists(configuration, path1);
		checkFileNotExists(configuration, path2);
		checkFileNotExists(configuration, path3);
		
		// create and check
		createFile(configuration, path1);
		
		manager.close();
	}

	@Test
	public void testTwoStandalone() throws Exception {
		YarnClusterManager manager = YarnClusterManager.getInstance();
		YarnCluster cluster1 = manager.getCluster(new ClusterInfo("def", 1, 1));
		YarnCluster cluster2 = manager.getCluster(new ClusterInfo("def", 2, 2));
		cluster1.start();
		cluster2.start();
		Configuration configuration1 = cluster1.getConfiguration();
		Configuration configuration2 = cluster2.getConfiguration();
		
		String rmAddress1 = configuration1.get(YarnConfiguration.RM_ADDRESS);
		String rmAddress2 = configuration2.get(YarnConfiguration.RM_ADDRESS);
		
		assertThat(rmAddress1, not(rmAddress2));
		
		// file from testOneStandalone can exist
		checkFileNotExists(configuration1, path1);
		checkFileNotExists(configuration2, path1);

		// these are not yet created
		checkFileNotExists(configuration1, path2);
		checkFileNotExists(configuration2, path3);
		
		// create and check
		createFile(configuration1, path2);
		createFile(configuration2, path3);
		
		manager.close();
	}

	@Test
	public void testTwoEquals() throws Exception {
		YarnClusterManager manager = YarnClusterManager.getInstance();
		YarnCluster cluster1 = manager.getCluster(new ClusterInfo("def", 1, 1));
		YarnCluster cluster2 = manager.getCluster(new ClusterInfo("def", 1, 1));
		cluster1.start();
		cluster2.start();
		Configuration configuration1 = cluster1.getConfiguration();
		Configuration configuration2 = cluster2.getConfiguration();
		
		String rmAddress1 = configuration1.get(YarnConfiguration.RM_ADDRESS);
		String rmAddress2 = configuration2.get(YarnConfiguration.RM_ADDRESS);
		
		assertThat(rmAddress1, is(rmAddress2));
		
		manager.close();
	}
	
	private void checkFileNotExists(Configuration configuration, String path) throws Exception {
		FileSystem fs = FileSystem.get(configuration);
		assertThat(fs.exists(new Path(path)), is(false));		
	}

	private void createFile(Configuration configuration, String path) throws Exception {
		FileSystem fs = FileSystem.get(configuration);
		FSDataOutputStream create = fs.create(new Path(path));
		create.close();
		assertThat(fs.exists(new Path(path)), is(true));		
	}
	
}
