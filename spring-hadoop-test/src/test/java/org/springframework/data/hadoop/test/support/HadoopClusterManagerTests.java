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
package org.springframework.data.hadoop.test.support;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.data.hadoop.test.HadoopTestSystemConstants;
import org.springframework.data.hadoop.test.context.HadoopCluster;

/**
 * Raw cluster tests through {@link HadoopClusterManager}.
 *
 * @author Janne Valkealahti
 *
 */
public class HadoopClusterManagerTests {

	private static String BASE = HadoopTestSystemConstants.HDFS_TESTS_BASE_PATH + "HadoopClusterManagerTests/";
	private static String path1 = BASE + "file1";
	private static String path2 = BASE + "file2";
	private static String path3 = BASE + "file3";

	@Test
	public void testOneStandalone() throws Exception {
		HadoopClusterManager manager = HadoopClusterManager.getInstance();
		HadoopCluster cluster = manager.getCluster(new ClusterInfo());
		cluster.start();

		Configuration configuration = cluster.getConfiguration();
		assertThat(getHdfsUrl(configuration), startsWith("hdfs"));

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
		HadoopClusterManager manager = HadoopClusterManager.getInstance();
		HadoopCluster cluster1 = manager.getCluster(new ClusterInfo("def", 1));
		HadoopCluster cluster2 = manager.getCluster(new ClusterInfo("def", 2));
		cluster1.start();
		cluster2.start();
		Configuration configuration1 = cluster1.getConfiguration();
		Configuration configuration2 = cluster2.getConfiguration();
		assertThat(getHdfsUrl(configuration1), startsWith("hdfs"));
		assertThat(getHdfsUrl(configuration2), startsWith("hdfs"));

		String fsUri1 = getHdfsUrl(configuration1);
		String fsUri2 = getHdfsUrl(configuration2);
		assertThat(fsUri1, notNullValue());
		assertThat(fsUri2, notNullValue());

		assertThat(fsUri1, not(fsUri2));

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
		HadoopClusterManager manager = HadoopClusterManager.getInstance();
		HadoopCluster cluster1 = manager.getCluster(new ClusterInfo("def", 1));
		HadoopCluster cluster2 = manager.getCluster(new ClusterInfo("def", 1));
		cluster1.start();
		cluster2.start();
		Configuration configuration1 = cluster1.getConfiguration();
		Configuration configuration2 = cluster2.getConfiguration();
		assertThat(getHdfsUrl(configuration1), startsWith("hdfs"));
		assertThat(getHdfsUrl(configuration2), startsWith("hdfs"));

		String fsUri1 = getHdfsUrl(configuration1);
		String fsUri2 = getHdfsUrl(configuration2);
		assertThat(fsUri1, notNullValue());
		assertThat(fsUri2, notNullValue());

		assertThat(fsUri1, is(fsUri2));

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

	private static String getHdfsUrl(Configuration configuration) {
		if (configuration.get("fs.defaultFS") != null) {
			return configuration.get("fs.defaultFS");
		} else {
			return null;
		}
	}

}
