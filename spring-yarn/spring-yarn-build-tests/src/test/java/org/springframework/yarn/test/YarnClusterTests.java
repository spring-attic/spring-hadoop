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
package org.springframework.yarn.test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.Timed;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.test.context.YarnCluster;

/**
 * Test for verifying that mini yarn cluster is
 * executed properly and a simple application
 * is uploaded and executed without failures.
 *
 * @author Janne Valkealahti
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@DirtiesContext(classMode=ClassMode.AFTER_CLASS)
public class YarnClusterTests {

	@Autowired
	private ApplicationContext ctx;

	@Test
	public void testConfiguredConfiguration() {
		assertTrue(ctx.containsBean("yarnConfiguration"));
		Configuration config = (Configuration) ctx.getBean("yarnConfiguration");
		assertNotNull(config);

		String rm = config.get(YarnConfiguration.RM_ADDRESS);
		String fs = config.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
		String scheduler = config.get(YarnConfiguration.RM_SCHEDULER_ADDRESS);
		assertNotNull(rm);
		assertNotNull(fs);
		assertNotNull(scheduler);
	}

	@Test
	@Timed(millis = 240000)
	public void testAppSubmission1() throws Exception {
		doSubmitAndAssert();
	}

	@Test
	@Timed(millis = 240000)
	public void testAppSubmission2() throws Exception {
		doSubmitAndAssert();
	}

	private void doSubmitAndAssert() throws Exception {
		YarnClient client = (YarnClient) ctx.getBean("yarnClient");
		assertThat(client, notNullValue());

		ApplicationId applicationId = client.submitApplication();
		assertThat(applicationId, notNullValue());

		YarnApplicationState state = null;
		for (int i = 0; i<240; i++) {
			state = findState(client, applicationId);
			if (state == null) {
				break;
			}
			if (state.equals(YarnApplicationState.FINISHED) || state.equals(YarnApplicationState.FAILED)) {
				break;
			}
			Thread.sleep(1000);
		}
		assertThat(state, notNullValue());

		YarnCluster cluster = (YarnCluster) ctx.getBean("yarnCluster");
		File testWorkDir = cluster.getYarnWorkDir();

		PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		String locationPattern = "file:" + testWorkDir.getAbsolutePath() + "/**/" + applicationId.toString() + "/**/*.std*";
		Resource[] resources = resolver.getResources(locationPattern);

		// get possible appmaster error from stderr file
		StringBuilder masterFailReason = new StringBuilder();
		for (Resource res : resources) {
			File file = res.getFile();
			if (file.getName().endsWith("Appmaster.stderr") && file.length() > 0) {
				Scanner scanner = new Scanner(file);
				masterFailReason.append("[Appmaster.stderr=");
				masterFailReason.append(scanner.useDelimiter("\\A").next());
				masterFailReason.append("]");
				scanner.close();
				break;
			}
		}

		masterFailReason.append(", [ApplicationReport Diagnostics=");
		masterFailReason.append(client.getApplicationReport(applicationId).getDiagnostics());
		masterFailReason.append("], [Num of log files=");
		masterFailReason.append(resources.length);
		masterFailReason.append("]");

		assertThat(masterFailReason.toString(), state, is(YarnApplicationState.FINISHED));

		// appmaster and 4 containers should
		// make it 10 log files
		assertThat(resources, notNullValue());
		assertThat(resources.length, is(10));

		for (Resource res : resources) {
			File file = res.getFile();
			if (file.getName().endsWith("stdout")) {
				assertThat(file.length(), greaterThan(0l));
			} else if (file.getName().endsWith("stderr")) {
				String content = "";
				if (file.length() > 0) {
					Scanner scanner = new Scanner(file);
					content = scanner.useDelimiter("\\A").next();
					scanner.close();
				}
				if (content.contains("Unable to load realm info from SCDynamicStore")) {
					// due to OS X giving 'Unable to load realm info from SCDynamicStore' errors we allow 100 bytes here
					assertThat(file.length(), lessThan(100l));
				} else {
					assertThat(file.length(), is(0l));
				}
			}
		}
	}

	private YarnApplicationState findState(YarnClient client, ApplicationId applicationId) {
		YarnApplicationState state = null;
		for (ApplicationReport report : client.listApplications()) {
			if (report.getApplicationId().equals(applicationId)) {
				state = report.getYarnApplicationState();
				break;
			}
		}
		return state;
	}

}
