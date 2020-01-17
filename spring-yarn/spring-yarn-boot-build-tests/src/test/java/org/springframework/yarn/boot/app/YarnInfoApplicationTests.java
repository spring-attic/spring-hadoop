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

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.yarn.boot.support.SpringYarnBootUtils;

public class YarnInfoApplicationTests extends AbstractApplicationTests {

	private final String BASE = "/tmp/YarnInfoApplicationTests/";

	private final static Log log = LogFactory.getLog(YarnInfoApplicationTests.class);

	@Test
	public void testListSubmitted() throws Exception {

		// minicluster, need to pass host/ports
		Properties appProperties = new Properties();
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "fs.defaultFS", "spring.hadoop.fsUri",
				appProperties);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.address",
				"spring.hadoop.resourceManagerAddress", appProperties);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.scheduler.address",
				"spring.hadoop.resourceManagerSchedulerAddress", appProperties);

		String[] args = new String[]{
				"--spring.yarn.internal.yarn-info-application.operation=SUBMITTED"
		};

		YarnInfoApplication app = new YarnInfoApplication();
		app.appProperties(appProperties);
		String info = app.run(args);
		log.info("info:\n" + info);
	}

	@Test
	public void testListInstalled() throws Exception {
		String ID = "testListInstalled";

		Path path = new Path(BASE + ID);
		FileSystem fs = path.getFileSystem(configuration);
		fs.mkdirs(path);

		// minicluster, need to pass host/ports
		Properties appProperties = new Properties();
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "fs.defaultFS", "spring.hadoop.fsUri",
				appProperties);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.address",
				"spring.hadoop.resourceManagerAddress", appProperties);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.scheduler.address",
				"spring.hadoop.resourceManagerSchedulerAddress", appProperties);

		String[] args = new String[]{
				"--spring.yarn.applicationBaseDir=" + BASE,
				"--spring.yarn.internal.yarn-info-application.operation=PUSHED"
		};

		YarnInfoApplication app = new YarnInfoApplication();
		app.appProperties(appProperties);
		app.applicationBaseDir(BASE);
		String info = app.run(args);
		log.info("info:\n" + info);
	}

}
