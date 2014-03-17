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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.Test;
import org.springframework.yarn.boot.support.SpringYarnBootUtils;

/**
 * Tests for {@link YarnKillApplication}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnKillApplicationTests extends AbstractApplicationTests {

	@Test
	public void testKill() throws Exception {
		String ID = "testKill";
		String BASE = "/tmp/YarnKillApplicationTests/";

		// install
		YarnInstallApplication installApp = new YarnInstallApplication();
		installApp.instanceId(ID);
		installApp.applicationBaseDir(BASE);

		Properties appProperties = new Properties();
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "fs.defaultFS", "spring.hadoop.fsUri",
				appProperties);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.address",
				"spring.hadoop.resourceManagerAddress", appProperties);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.scheduler.address",
				"spring.hadoop.resourceManagerSchedulerAddress", appProperties);

		installApp.configFile("application.properties", appProperties);
		installApp.appProperties(appProperties);

		String[] installArgs = new String[]{
				"--spring.yarn.appmaster.appmasterClass=org.springframework.yarn.boot.app.StartSleepAppmaster",
				"--spring.yarn.client.clientClass=org.springframework.yarn.client.DefaultApplicationYarnClient",
				"--spring.yarn.client.files[0]=" + APPMASTER_ARCHIVE_PATH
		};
		installApp.run(installArgs);

		// submit
		YarnSubmitApplication submitApp = new YarnSubmitApplication();
		submitApp.instanceId(ID);
		submitApp.applicationBaseDir(BASE);
		submitApp.appProperties(appProperties);

		String[] submitArgs = new String[]{
				"--spring.yarn.client.clientClass=org.springframework.yarn.client.DefaultApplicationYarnClient",
				"--spring.yarn.client.launchcontext.archiveFile=" + APPMASTER_ARCHIVE
		};

		ApplicationId applicationId = submitApp.run(submitArgs);

		YarnApplicationState state = waitState(applicationId, 60, TimeUnit.SECONDS, YarnApplicationState.FINISHED,
				YarnApplicationState.FAILED, YarnApplicationState.RUNNING);
		assertThat(state, is(YarnApplicationState.RUNNING));

		// kill
		YarnKillApplication killApp = new YarnKillApplication();
		killApp.appProperties(appProperties);

		String[] killArgs = new String[]{
				"--spring.yarn.internal.YarnKillApplication.applicationId=" + applicationId.toString()
		};

		String info = killApp.run(killArgs);
		assertThat(info, containsString("Kill request for"));
	}

}
