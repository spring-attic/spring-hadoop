/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.yarn.boot.app;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.After;
import org.junit.Before;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.client.YarnClientFactoryBean;
import org.springframework.yarn.test.context.YarnCluster;
import org.springframework.yarn.test.support.ClusterInfo;
import org.springframework.yarn.test.support.YarnClusterManager;

/**
 * Common shared stuff handling for application tests. For example we use rather
 * low level minicluster handling here to have hdfs/yarn system setup for tests.
 * In tests we want to be as close as possible for real use case so we don't
 * even try to use any magic from rest of the testing support.
 * <p>
 * Tests derived from this class will assume existence of
 * build/libs/test-archive-appmaster.jar which is a custom boot packaged jar
 * having a dummy StartExitAppmaster class.
 * <p>
 * We don't run any containers because focus on these tests is to test app
 * install/submit/info/query classes.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractApplicationTests {

	private final static Log log = LogFactory.getLog(AbstractApplicationTests.class);

	protected final static String APPMASTER_ARCHIVE = "test-archive-appmaster.jar";
	protected final static String APPMASTER_ARCHIVE_PATH = "file:build/libs/" + APPMASTER_ARCHIVE;

	protected YarnCluster cluster;
	protected Configuration configuration;

	@Before
	public void setup() throws Exception {
		YarnClusterManager manager = YarnClusterManager.getInstance();
		cluster = manager.getCluster(new ClusterInfo());
		cluster.start();
		configuration = cluster.getConfiguration();
	}

	@After
	public void clean() {
		if (cluster != null) {
			YarnClusterManager manager = YarnClusterManager.getInstance();
			manager.close();
			cluster = null;
		}
		configuration = null;
	}

	protected void listFiles(Configuration configuration) {
		@SuppressWarnings("resource")
		FsShell shell = new FsShell(configuration);
		for (FileStatus s : shell.ls(true, "/")) {
			log.info("status " + s);
		}
	}

	protected void catFile(String path) {
		@SuppressWarnings("resource")
		FsShell shell = new FsShell(configuration);
		Collection<String> text = shell.text(path);
		if (text.size() == 1) {
			log.info(text.iterator().next());
		}
	}

	protected Properties readApplicationProperties(Path path) throws IOException {
		FileSystem fs = null;
		FSDataInputStream in = null;
		Properties properties = null;
		IOException ioe = null;
		try {
			fs = path.getFileSystem(configuration);
			if (fs.exists(path)) {
				in = fs.open(path);
				properties = new Properties();
				properties.load(in);
			}
		}
		catch (IOException e) {
			ioe = e;
		}
		finally {
			if (in != null) {
				try {
					in.close();
					in = null;
				}
				catch (IOException e) {
				}
			}
			fs = null;
		}
		if (ioe != null) {
			throw ioe;
		}
		return properties;
	}

	protected YarnApplicationState waitState(ApplicationId applicationId, long timeout, TimeUnit unit,
			YarnApplicationState... applicationStates) throws Exception {
		YarnApplicationState state = null;
		long end = System.currentTimeMillis() + unit.toMillis(timeout);

		// break label for inner loop
		done: do {
			state = findState(getYarnClient(), applicationId);
			if (state == null) {
				break;
			}
			for (YarnApplicationState stateCheck : applicationStates) {
				if (state.equals(stateCheck)) {
					break done;
				}
			}
			Thread.sleep(1000);
		} while (System.currentTimeMillis() < end);
		return state;
	}

	protected YarnApplicationState findState(YarnClient client, ApplicationId applicationId) {
		YarnApplicationState state = null;
		for (ApplicationReport report : client.listApplications()) {
			if (report.getApplicationId().equals(applicationId)) {
				state = report.getYarnApplicationState();
				break;
			}
		}
		return state;
	}

	protected YarnClient getYarnClient() throws Exception {
		YarnClientFactoryBean factory = new YarnClientFactoryBean();
		factory.setConfiguration(configuration);
		factory.afterPropertiesSet();
		return factory.getObject();
	}

	protected ApplicationReport findApplicationReport(ApplicationId applicationId) throws Exception {
		YarnClient client = getYarnClient();
		for (ApplicationReport report : client.listApplications()) {
			if (report.getApplicationId().equals(applicationId)) {
				client = null;
				return report;
			}
		}
		client = null;
		return null;
	}

}
