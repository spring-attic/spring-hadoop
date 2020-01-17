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
package org.springframework.yarn.test.junit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import java.io.File;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.Test;
import org.springframework.core.io.Resource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.Timed;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.yarn.test.context.MiniYarnCluster;
import org.springframework.yarn.test.context.YarnDelegatingSmartContextLoader;
import org.springframework.yarn.test.support.ContainerLogUtils;

/**
 * Cluster tests where we basically submit two same applications using same
 * context without doing any context dirtying. Apps should run in a same cluster
 * instance.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration(loader = YarnDelegatingSmartContextLoader.class)
@MiniYarnCluster
@DirtiesContext(classMode=ClassMode.AFTER_CLASS)
public class ClusterBaseTestClassSubmitTests extends AbstractYarnClusterTests {

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
		ApplicationInfo info = submitApplicationAndWait(240, TimeUnit.SECONDS);
		assertThat(info, notNullValue());
		assertThat(info.getYarnApplicationState(), notNullValue());
		assertThat(info.getApplicationId(), notNullValue());
		assertThat(info.getYarnApplicationState(), is(YarnApplicationState.FINISHED));

		List<Resource> resources = ContainerLogUtils.queryContainerLogs(getYarnCluster(),
				info.getApplicationId());

		// appmaster and 4 containers should
		// make it 20 log files
		assertThat(resources, notNullValue());
		assertThat("expecting 20 log files", resources.size(), is(20));

		for (Resource res : resources) {
			File file = res.getFile();
			if (file.getName().endsWith(".stdout")) {
				// there has to be some content in stdout file
				assertThat("there has to be content in stdout file", file.length(), greaterThan(0l));
				if (file.getName().equals("Container.stdout")) {
					Scanner scanner = new Scanner(file);
					String content = scanner.useDelimiter("\\A").next();
					scanner.close();
					// check that we have a simple timestamp
					assertThat("content doesn't look like timestamp", content.length(), greaterThan(10));
					assertThat("content doesn't look like timestamp", content.length(), lessThan(40));
				}
			} else if (file.getName().endsWith(".stderr")) {
				String content = "";
				if (file.length() > 0) {
					Scanner scanner = new Scanner(file);
					content = scanner.useDelimiter("\\A").next();
					scanner.close();
				}
				if (content.contains("Unable to load realm info from SCDynamicStore")) {
					// due to OS X giving 'Unable to load realm info from SCDynamicStore' errors we allow 100 bytes here
					assertThat("stderr file is not empty: " + content, file.length(), lessThan(100l));
				} else {
					// can't have anything in stderr files
					assertThat("stderr file is not empty: " + content, file.length(), is(0l));
				}
			} else if (file.getName().endsWith(".err")) {
			    assertEquals("prelaunch.err", file.getName());
			    assertThat("prelaunch.err file is not empty", file.length(), is(0l));
			} else if (file.getName().endsWith(".out")) {
			  assertEquals("prelaunch.out", file.getName());
            } else {
              throw new Exception("Unkonwn log file: " + file);
            }
		}

	}

}
