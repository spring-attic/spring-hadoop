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
package org.springframework.yarn.config;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertNotNull;
import java.util.List;
import java.util.Map;
import javax.annotation.Resource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.yarn.TestUtils;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.client.YarnClientFactoryBean;
import org.springframework.yarn.fs.ResourceLocalizer;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("client-ns.xml")
public class ClientNamespaceTest {

	@Resource(name = "yarnClient")
	private YarnClient defaultYarnClient;

	@Resource(name = "&yarnClient")
	private YarnClientFactoryBean defaultYarnClientFactoryBean;

	@Resource(name = "customClient")
	private YarnClient customYarnClient;

	@Resource(name = "&customClient")
	private YarnClientFactoryBean customYarnClientFactoryBean;

	@Resource(name = "customClientWithRunnerDefaults")
	private YarnClient customYarnClientWithRunnerDefaults;

	@Resource(name = "&customClientWithRunnerDefaults")
	private YarnClientFactoryBean customYarnClientWithRunnerDefaultsFactoryBean;

	@Resource(name = "customClientWithRunnerCustoms")
	private YarnClient customYarnClientWithRunnerCustoms;

	@Resource(name = "&customClientWithRunnerCustoms")
	private YarnClientFactoryBean customYarnClientWithRunnerCustomsFactoryBean;

	@Test
	public void testDefaults() throws Exception {
		assertNotNull(defaultYarnClient);
		assertNotNull(defaultYarnClientFactoryBean);

		Configuration configuration = TestUtils.readField("configuration", defaultYarnClientFactoryBean);
		assertNotNull(configuration);
		String defaultFs = configuration.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
		assertThat(defaultFs, startsWith("file"));

		Map<String, String> environment = TestUtils.readField("environment", defaultYarnClientFactoryBean);
		assertNotNull(environment);

		ResourceLocalizer resourceLocalizer = TestUtils.readField("resourceLocalizer", defaultYarnClientFactoryBean);
		assertNotNull(resourceLocalizer);

		List<String> commands  = TestUtils.readField("commands", defaultYarnClientFactoryBean);
		assertNotNull(commands);
		assertThat(commands.size(), is(1));
		assertThat(commands.get(0).trim(), is("fakecommand"));

		String appName = TestUtils.readField("appName", defaultYarnClientFactoryBean);
		assertThat(appName, is(""));
	}

	@Test
	public void testCustom() throws Exception {
		assertNotNull(customYarnClient);
		assertNotNull(customYarnClientFactoryBean);

		Configuration configuration = TestUtils.readField("configuration", customYarnClientFactoryBean);
		assertNotNull(configuration);
		String defaultFs = configuration.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
		assertThat(defaultFs, startsWith("file"));

		Map<String, String> environment = TestUtils.readField("environment", customYarnClientFactoryBean);
		assertNotNull(environment);

		ResourceLocalizer resourceLocalizer = TestUtils.readField("resourceLocalizer", customYarnClientFactoryBean);
		assertNotNull(resourceLocalizer);

		List<String> commands  = TestUtils.readField("commands", customYarnClientFactoryBean);
		assertNotNull(commands);
		assertThat(commands.size(), is(1));
		assertThat(commands.get(0).trim(), is("customcommand"));

		String appName = TestUtils.readField("appName", customYarnClientFactoryBean);
		assertThat(appName, is("customAppName"));

		Integer priority = TestUtils.readField("priority", customYarnClientFactoryBean);
		assertThat(priority, is(1));

		Long memory = TestUtils.readField("memory", customYarnClientFactoryBean);
		assertThat(memory, is(11L));

		String queue = TestUtils.readField("queue", customYarnClientFactoryBean);
		assertThat(queue, is("customqueue"));

		Integer virtualcores = TestUtils.readField("virtualcores", customYarnClientFactoryBean);
		assertThat(virtualcores, is(2));
	}

	@Test
	public void testCustomClientWithRunnerDefaults() throws Exception {
		assertNotNull(customYarnClientWithRunnerDefaults);
		assertNotNull(customYarnClientWithRunnerDefaultsFactoryBean);

		List<String> commands  = TestUtils.readField("commands", customYarnClientWithRunnerDefaultsFactoryBean);
		assertNotNull(commands);
		assertThat(commands.size(), is(6));
		assertThat(commands.get(0).trim(), containsString("JAVA_HOME"));
		assertThat(commands.get(1).trim(), is("org.springframework.yarn.am.CommandLineAppmasterRunner"));
		assertThat(commands.get(2).trim(), is("appmaster-context.xml"));
		assertThat(commands.get(3).trim(), is("yarnAppmaster"));
	}

	@Test
	public void testCustomClientWithRunnerCustoms() throws Exception {
		assertNotNull(customYarnClientWithRunnerCustoms);
		assertNotNull(customYarnClientWithRunnerCustomsFactoryBean);

		List<String> commands  = TestUtils.readField("commands", customYarnClientWithRunnerCustomsFactoryBean);
		assertNotNull(commands);
		assertThat(commands.size(), is(8));
		assertThat(commands.get(0).trim(), is("command"));
		assertThat(commands.get(1).trim(), is("org.springframework.yarn.am.CommandLineAppmasterRunner"));
		assertThat(commands.get(2).trim(), is("contextFile"));
		assertThat(commands.get(3).trim(), is("beanName"));
		assertThat(commands.get(4).trim(), is("foo21=jee21"));
		assertThat(commands.get(5).trim(), is("foo20=jee20"));
	}

}
