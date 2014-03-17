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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.yarn.boot.SpringApplicationException;
import org.springframework.yarn.boot.support.SpringYarnBootUtils;

/**
 * Tests for {@link YarnInstallApplication}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnInstallApplicationTests extends AbstractApplicationTests {

	private final String BASE = "/tmp/YarnBootClientInstallApplicationTests/";

	@Test
	public void testEmptyInstall() throws Exception {
		String ID = "testEmptyInstall";

		YarnInstallApplication app = new YarnInstallApplication();
		app.instanceId(ID);
		app.applicationBaseDir(BASE);

		// minicluster, need to pass host/ports
		Properties appProperties = new Properties();
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "fs.defaultFS", "spring.hadoop.fsUri",
				appProperties);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.address",
				"spring.hadoop.resourceManagerAddress", appProperties);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.scheduler.address",
				"spring.hadoop.resourceManagerSchedulerAddress", appProperties);
		app.appProperties(appProperties);

		String[] args = new String[]{
				"--spring.yarn.client.clientClass=org.springframework.yarn.client.DefaultApplicationYarnClient",
		};

		app.run(args);

		listFiles(configuration);

	}

	@Test
	public void testInstall() throws Exception {
		String ID = "testInstall";

		YarnInstallApplication app = new YarnInstallApplication();
		app.instanceId(ID);
		app.applicationBaseDir(BASE);

		Properties appProperties = new Properties();
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "fs.defaultFS", "spring.hadoop.fsUri",
				appProperties);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.address",
				"spring.hadoop.resourceManagerAddress", appProperties);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.scheduler.address",
				"spring.hadoop.resourceManagerSchedulerAddress", appProperties);

		app.configFile("application.properties", appProperties);
		app.appProperties(appProperties);

		String[] args = new String[]{
				"--spring.yarn.client.clientClass=org.springframework.yarn.client.DefaultApplicationYarnClient",
				"--spring.yarn.client.files[0]=" + APPMASTER_ARCHIVE_PATH
		};

		app.run(args);

		listFiles(configuration);
		catFile(BASE + ID + "/application.properties");

		Path archive = new Path(BASE + ID + "/" + APPMASTER_ARCHIVE);
		FileSystem fs = archive.getFileSystem(configuration);
		assertThat(fs.exists(archive), is(true));

		Properties applicationProperties = readApplicationProperties(new Path(BASE + ID + "/application.properties"));
		assertThat(applicationProperties.size(), is(3));
		assertThat(applicationProperties.get("spring.hadoop.fsUri"), notNullValue());
		assertThat(applicationProperties.get("spring.hadoop.resourceManagerSchedulerAddress"), notNullValue());
		assertThat(applicationProperties.get("spring.hadoop.resourceManagerAddress"), notNullValue());
	}

	@Test(expected=SpringApplicationException.class)
	public void testInstallFailureIdMissing() throws Exception {
		YarnInstallApplication app = new YarnInstallApplication();
		String[] args = new String[]{
				"--spring.yarn.client.files[0]=" + APPMASTER_ARCHIVE_PATH
		};
		app.run(args);
	}

	@Test(expected=SpringApplicationException.class)
	public void testInstallFailureAppAlreadyInstalled() throws Exception {
		String ID = "testInstallFailureAppAlreadyInstalled";

		Path path = new Path(BASE + ID);
		FileSystem fs = path.getFileSystem(configuration);
		fs.mkdirs(path);

		YarnInstallApplication app = new YarnInstallApplication();
		app.instanceId(ID);
		app.applicationBaseDir(BASE);

		Properties properties = new Properties();
		SpringYarnBootUtils
				.mergeHadoopPropertyIntoMap(configuration, "fs.defaultFS", "spring.hadoop.fsUri", properties);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.address",
				"spring.hadoop.resourceManagerAddress", properties);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.scheduler.address",
				"spring.hadoop.resourceManagerSchedulerAddress", properties);

		app.configFile("application.properties", properties);
		app.appProperties(properties);

		String[] args = new String[]{
				"--spring.yarn.client.clientClass=org.springframework.yarn.client.DefaultApplicationYarnClient",
				"--spring.yarn.client.files[0]=" + APPMASTER_ARCHIVE_PATH
		};

		app.run(args);
	}

}
