/*
 * Copyright 2014-2016 the original author or authors.
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
package org.springframework.yarn.boot.properties;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.yarn.boot.properties.SpringYarnAppmasterProperties.ContainerClustersProjectionProperties;

/**
 * Tests for {@link SpringYarnAppmasterContainerClusterProperties} bindings.
 *
 * @author Janne Valkealahti
 *
 */
public class SpringYarnAppmasterContainerClusterPropertiesTests {

	@Test
	public void testAllPropertiesSet() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringYarnAppmasterContainerClusterPropertiesTests1" });
		SpringYarnAppmasterProperties properties = context.getBean(SpringYarnAppmasterProperties.class);
		assertThat(properties, notNullValue());

		assertThat(properties.getContainercluster().getClusters().size(), is(2));

		ContainerClustersProjectionProperties projectionProperties = properties.getContainercluster().getClusters().get("cluster1").getProjection();
		assertThat(projectionProperties.getType(), is("default"));
		assertThat(projectionProperties.getData().getAny(), is(1));
		assertThat(projectionProperties.getData().getHosts().get("host1"), is(1));
		assertThat(projectionProperties.getData().getHosts().get("host2"), is(2));
		assertThat(projectionProperties.getData().getRacks().get("rack1"), is(1));
		assertThat(projectionProperties.getData().getRacks().get("rack2"), is(2));
		assertThat((String)projectionProperties.getData().getProperties().get("foo"), is("bar"));

		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource(), notNullValue());
		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource().getPriority(), is(234));
		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource().getMemory(), is("memoryFoo"));
		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource().getVirtualCores(), is(123));
		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource().getLabelExpression(), is("appLabelExpressionFoo1"));

		assertThat(properties.getContainercluster().getClusters().get("cluster1").getLaunchcontext(), notNullValue());
		SpringYarnAppmasterLaunchContextProperties properties1 = properties.getContainercluster().getClusters().get("cluster1").getLaunchcontext();
		assertThat(properties1, notNullValue());
		assertThat(properties1.getArchiveFile(), is("archiveFileFoo"));
		Map<String, String> arguments = properties1.getArguments();
		assertThat(arguments, notNullValue());
		assertThat(arguments.size(), is(2));
		assertThat(arguments.get("argumentsKeyFoo1"), is("argumentsValFoo1"));
		assertThat(arguments.get("argumentsKeyFoo2"), is("argumentsValFoo2"));

		List<String> argumentsList = properties1.getArgumentsList();
		assertThat(argumentsList, notNullValue());
		assertThat(argumentsList.size(), is(2));
		assertThat(argumentsList, contains("argumentsListFoo1", "argumentsListFoo2"));

		List<String> classpath = properties1.getContainerAppClasspath();
		assertThat(classpath, notNullValue());
		assertThat(classpath.size(), is(2));
		assertThat(classpath.get(0), is("classpath1Foo"));
		assertThat(classpath.get(1), is("classpath2Foo"));
		assertThat(properties1.getRunnerClass(), is("runnerClassFoo"));
		List<String> options = properties1.getOptions();
		assertThat(options, notNullValue());
		assertThat(options.size(), is(2));
		assertThat(options.get(0), is("options1Foo"));
		assertThat(options.get(1), is("options2Foo"));
		assertThat(properties1.isLocality(), is(true));
		assertThat(properties1.isUseYarnAppClasspath(), is(false));
		assertThat(properties1.isIncludeBaseDirectory(), is(false));
		assertThat(properties1.isIncludeLocalSystemEnv(), is(true));
		assertThat(properties1.getPathSeparator(), is(":"));

		assertThat(properties.getContainercluster().getClusters().get("cluster1").getLocalizer(), notNullValue());
		SpringYarnAppmasterLocalizerProperties properties2 = properties.getContainercluster().getClusters().get("cluster1").getLocalizer();
		assertThat(properties2, notNullValue());
		List<String> pattern = properties2.getPatterns();
		assertThat(pattern, notNullValue());
		assertThat(pattern.size(), is(2));
		assertThat(pattern.get(0), is("patterns1Foo"));
		assertThat(pattern.get(1), is("patterns2Foo"));
		List<String> names = properties2.getPropertiesNames();
		assertThat(names, notNullValue());
		assertThat(names.size(), is(2));
		assertThat(names.get(0), is("name1Foo"));
		assertThat(names.get(1), is("name2Foo"));
		List<String> suffixes = properties2.getPropertiesSuffixes();
		assertThat(suffixes, notNullValue());
		assertThat(suffixes.size(), is(2));
		assertThat(suffixes.get(0), is("suffix1Foo"));
		assertThat(suffixes.get(1), is("suffix2Foo"));
		assertThat(properties2.getZipPattern(), is("zipPatternFoo"));

		assertThat(properties.getContainercluster().getClusters().get("cluster2").getResource(), notNullValue());
		assertThat(properties.getContainercluster().getClusters().get("cluster2").getLaunchcontext(), nullValue());
		assertThat(properties.getContainercluster().getClusters().get("cluster2").getLocalizer(), nullValue());
		assertThat(properties.getContainercluster().getClusters().get("cluster2").getResource().getPriority(), is(2344));
		assertThat(properties.getContainercluster().getClusters().get("cluster2").getResource().getMemory(), is("memoryFooo"));
		assertThat(properties.getContainercluster().getClusters().get("cluster2").getResource().getVirtualCores(), is(1233));
		assertThat(properties.getContainercluster().getClusters().get("cluster2").getResource().getLabelExpression(), is("appLabelExpressionFoo2"));
		context.close();
	}

	@Test
	public void testAllPropertiesSet2() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringYarnAppmasterContainerClusterPropertiesTests2" });
		SpringYarnAppmasterProperties properties = context.getBean(SpringYarnAppmasterProperties.class);
		assertThat(properties, notNullValue());

		assertThat(properties.getContainercluster().getClusters().size(), is(1));

		ContainerClustersProjectionProperties projectionProperties = properties.getContainercluster().getClusters().get("cluster1").getProjection();
		assertThat(projectionProperties.getType(), is("default"));
		assertThat(projectionProperties.getData().getAny(), is(1));
		assertThat(projectionProperties.getData().getHosts().get("host1"), is(1));
		assertThat(projectionProperties.getData().getHosts().get("host2"), is(2));
		assertThat(projectionProperties.getData().getRacks().get("rack1"), is(1));
		assertThat(projectionProperties.getData().getRacks().get("rack2"), is(2));
		assertThat((String)projectionProperties.getData().getProperties().get("foo"), is("bar"));

		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource(), notNullValue());
		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource().getPriority(), is(234));
		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource().getMemory(), is("memoryFoo"));
		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource().getVirtualCores(), is(123));

		assertThat(properties.getContainercluster().getClusters().get("cluster1").getLaunchcontext(), notNullValue());
		SpringYarnAppmasterLaunchContextProperties properties1 = properties.getContainercluster().getClusters().get("cluster1").getLaunchcontext();
		assertThat(properties1, notNullValue());
		assertThat(properties1.getArchiveFile(), is("archiveFileFoo"));
		Map<String, String> arguments = properties1.getArguments();
		assertThat(arguments, notNullValue());
		assertThat(arguments.size(), is(2));
		assertThat(arguments.get("argumentsKeyFoo1"), is("argumentsValFoo1"));
		assertThat(arguments.get("argumentsKeyFoo2"), is("argumentsValFoo2"));
		List<String> classpath = properties1.getContainerAppClasspath();
		assertThat(classpath, notNullValue());
		assertThat(classpath.size(), is(2));
		assertThat(classpath.get(0), is("classpath1Foo"));
		assertThat(classpath.get(1), is("classpath2Foo"));
		assertThat(properties1.getRunnerClass(), is("runnerClassFoo"));
		List<String> options = properties1.getOptions();
		assertThat(options, notNullValue());
		assertThat(options.size(), is(2));
		assertThat(options.get(0), is("options1Foo"));
		assertThat(options.get(1), is("options2Foo"));
		assertThat(properties1.isLocality(), is(true));
		assertThat(properties1.isUseYarnAppClasspath(), is(false));
		assertThat(properties1.isIncludeBaseDirectory(), is(false));
		assertThat(properties1.isIncludeLocalSystemEnv(), is(true));
		assertThat(properties1.getPathSeparator(), is(":"));

		assertThat(properties.getContainercluster().getClusters().get("cluster1").getLocalizer(), notNullValue());
		SpringYarnAppmasterLocalizerProperties properties2 = properties.getContainercluster().getClusters().get("cluster1").getLocalizer();
		assertThat(properties2, notNullValue());
		List<String> pattern = properties2.getPatterns();
		assertThat(pattern, notNullValue());
		assertThat(pattern.size(), is(2));
		assertThat(pattern.get(0), is("patterns1Foo"));
		assertThat(pattern.get(1), is("patterns2Foo"));
		List<String> names = properties2.getPropertiesNames();
		assertThat(names, notNullValue());
		assertThat(names.size(), is(2));
		assertThat(names.get(0), is("name1Foo"));
		assertThat(names.get(1), is("name2Foo"));
		List<String> suffixes = properties2.getPropertiesSuffixes();
		assertThat(suffixes, notNullValue());
		assertThat(suffixes.size(), is(2));
		assertThat(suffixes.get(0), is("suffix1Foo"));
		assertThat(suffixes.get(1), is("suffix2Foo"));
		assertThat(properties2.getZipPattern(), is("zipPatternFoo"));

		context.close();
	}

	@Test
	public void testAllMultiYaml() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringYarnAppmasterContainerClusterPropertiesTests2",
						"--spring.config.additional-location=classpath:/SpringYarnAppmasterContainerClusterPropertiesTests2-2.yml"});
		SpringYarnAppmasterProperties properties = context.getBean(SpringYarnAppmasterProperties.class);
		assertThat(properties, notNullValue());

		assertThat(properties.getContainercluster().getClusters().size(), is(2));

		ContainerClustersProjectionProperties projectionProperties = properties.getContainercluster().getClusters().get("cluster1").getProjection();
		assertThat(projectionProperties.getType(), is("default"));
		assertThat(projectionProperties.getData().getAny(), is(1));
		assertThat(projectionProperties.getData().getHosts().get("host1"), is(1));
		assertThat(projectionProperties.getData().getHosts().get("host2"), is(2));
		assertThat(projectionProperties.getData().getRacks().get("rack1"), is(1));
		assertThat(projectionProperties.getData().getRacks().get("rack2"), is(2));
		assertThat((String)projectionProperties.getData().getProperties().get("foo"), is("bar"));

		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource(), notNullValue());
		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource().getPriority(), is(234));
		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource().getMemory(), is("memoryFoo"));
		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource().getVirtualCores(), is(123));

		assertThat(properties.getContainercluster().getClusters().get("cluster1").getLaunchcontext(), notNullValue());
		SpringYarnAppmasterLaunchContextProperties properties1 = properties.getContainercluster().getClusters().get("cluster1").getLaunchcontext();
		assertThat(properties1, notNullValue());
		assertThat(properties1.getArchiveFile(), is("archiveFileFoo"));
		Map<String, String> arguments = properties1.getArguments();
		assertThat(arguments, notNullValue());
		assertThat(arguments.size(), is(2));
		assertThat(arguments.get("argumentsKeyFoo1"), is("argumentsValFoo1"));
		assertThat(arguments.get("argumentsKeyFoo2"), is("argumentsValFoo2"));
		List<String> classpath = properties1.getContainerAppClasspath();
		assertThat(classpath, notNullValue());
		assertThat(classpath.size(), is(2));
		assertThat(classpath.get(0), is("classpath1Foo"));
		assertThat(classpath.get(1), is("classpath2Foo"));
		assertThat(properties1.getRunnerClass(), is("runnerClassFoo"));
		List<String> options = properties1.getOptions();
		assertThat(options, notNullValue());
		assertThat(options.size(), is(2));
		assertThat(options.get(0), is("options1Foo"));
		assertThat(options.get(1), is("options2Foo"));
		assertThat(properties1.isLocality(), is(true));
		assertThat(properties1.isUseYarnAppClasspath(), is(false));
		assertThat(properties1.isIncludeBaseDirectory(), is(false));
		assertThat(properties1.isIncludeLocalSystemEnv(), is(true));
		assertThat(properties1.getPathSeparator(), is(":"));

		assertThat(properties.getContainercluster().getClusters().get("cluster1").getLocalizer(), notNullValue());
		SpringYarnAppmasterLocalizerProperties properties2 = properties.getContainercluster().getClusters().get("cluster1").getLocalizer();
		assertThat(properties2, notNullValue());
		List<String> pattern = properties2.getPatterns();
		assertThat(pattern, notNullValue());
		assertThat(pattern.size(), is(2));
		assertThat(pattern.get(0), is("patterns1Foo"));
		assertThat(pattern.get(1), is("patterns2Foo"));
		List<String> names = properties2.getPropertiesNames();
		assertThat(names, notNullValue());
		assertThat(names.size(), is(2));
		assertThat(names.get(0), is("name1Foo"));
		assertThat(names.get(1), is("name2Foo"));
		List<String> suffixes = properties2.getPropertiesSuffixes();
		assertThat(suffixes, notNullValue());
		assertThat(suffixes.size(), is(2));
		assertThat(suffixes.get(0), is("suffix1Foo"));
		assertThat(suffixes.get(1), is("suffix2Foo"));
		assertThat(properties2.getZipPattern(), is("zipPatternFoo"));

		projectionProperties = properties.getContainercluster().getClusters().get("cluster2").getProjection();
		assertThat(projectionProperties.getType(), is("default"));
		assertThat(projectionProperties.getData().getAny(), is(2));

		context.close();
	}

	@Test
	public void testAllPropertiesSet3() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringYarnAppmasterContainerClusterPropertiesTests3" });
		SpringYarnAppmasterProperties properties = context.getBean(SpringYarnAppmasterProperties.class);
		assertThat(properties, notNullValue());

		assertThat(properties.getContainercluster().getClusters().size(), is(1));

		ContainerClustersProjectionProperties projectionProperties = properties.getContainercluster().getClusters().get("cluster1").getProjection();
		assertThat(projectionProperties.getType(), is("default"));
		assertThat(projectionProperties.getData().getAny(), is(1));
		assertThat(projectionProperties.getData().getHosts().get("host1"), is(1));
		assertThat(projectionProperties.getData().getHosts().get("host2"), is(2));
		assertThat(projectionProperties.getData().getRacks().get("rack1"), is(1));
		assertThat(projectionProperties.getData().getRacks().get("rack2"), is(2));
		assertThat((String)projectionProperties.getData().getProperties().get("foo"), is("bar"));

		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource(), notNullValue());
		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource().getPriority(), is(234));
		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource().getMemory(), is("memoryFoo"));
		assertThat(properties.getContainercluster().getClusters().get("cluster1").getResource().getVirtualCores(), is(123));

		assertThat(properties.getContainercluster().getClusters().get("cluster1").getLaunchcontext(), notNullValue());
		SpringYarnAppmasterLaunchContextProperties properties1 = properties.getContainercluster().getClusters().get("cluster1").getLaunchcontext();
		assertThat(properties1, notNullValue());
		assertThat(properties1.getArchiveFile(), is("archiveFileFoo"));
		Map<String, String> arguments = properties1.getArguments();
		assertThat(arguments, notNullValue());
		assertThat(arguments.size(), is(2));
		assertThat(arguments.get("argumentsKeyFoo1"), is("argumentsValFoo1"));
		assertThat(arguments.get("argumentsKeyFoo2"), is("argumentsValFoo2"));
		List<String> classpath = properties1.getContainerAppClasspath();
		assertThat(classpath, notNullValue());
		assertThat(classpath.size(), is(2));
		assertThat(classpath.get(0), is("classpath1Foo"));
		assertThat(classpath.get(1), is("classpath2Foo"));
		assertThat(properties1.getRunnerClass(), is("runnerClassFoo"));
		List<String> options = properties1.getOptions();
		assertThat(options, notNullValue());
		assertThat(options.size(), is(2));
		assertThat(options.get(0), is("options1Foo"));
		assertThat(options.get(1), is("options2Foo"));
		assertThat(properties1.isLocality(), is(true));
		assertThat(properties1.isUseYarnAppClasspath(), is(false));
		assertThat(properties1.isIncludeBaseDirectory(), is(false));
		assertThat(properties1.isIncludeLocalSystemEnv(), is(true));
		assertThat(properties1.getPathSeparator(), is(":"));

		assertThat(properties.getContainercluster().getClusters().get("cluster1").getLocalizer(), notNullValue());
		SpringYarnAppmasterLocalizerProperties properties2 = properties.getContainercluster().getClusters().get("cluster1").getLocalizer();
		assertThat(properties2, notNullValue());
		List<String> pattern = properties2.getPatterns();
		assertThat(pattern, notNullValue());
		assertThat(pattern.size(), is(2));
		assertThat(pattern.get(0), is("patterns1Foo"));
		assertThat(pattern.get(1), is("patterns2Foo"));
		List<String> names = properties2.getPropertiesNames();
		assertThat(names, notNullValue());
		assertThat(names.size(), is(2));
		assertThat(names.get(0), is("name1Foo"));
		assertThat(names.get(1), is("name2Foo"));
		List<String> suffixes = properties2.getPropertiesSuffixes();
		assertThat(suffixes, notNullValue());
		assertThat(suffixes.size(), is(2));
		assertThat(suffixes.get(0), is("suffix1Foo"));
		assertThat(suffixes.get(1), is("suffix2Foo"));
		assertThat(properties2.getZipPattern(), is("zipPatternFoo"));

		context.close();
	}

	@Test
	public void testMapStylePropertiesSet4() {
		SpringApplication app = new SpringApplication(TestConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app
				.run(new String[] { "--spring.config.name=SpringYarnAppmasterContainerClusterPropertiesTests4" });
		SpringYarnAppmasterProperties properties = context.getBean(SpringYarnAppmasterProperties.class);
		assertThat(properties, notNullValue());

		assertThat(properties.getContainercluster().getClusters().size(), is(1));

		ContainerClustersProjectionProperties projectionProperties = properties.getContainercluster().getClusters().get("cluster1").getProjection();
		assertThat(projectionProperties.getType(), is("default"));
		assertThat(projectionProperties.getData().getAny(), is(1));
		assertThat(projectionProperties.getData().getHosts().get("host1"), is(1));
		assertThat(projectionProperties.getData().getHosts().get("host2"), is(2));
		assertThat(projectionProperties.getData().getRacks().get("rack1"), is(1));
		assertThat(projectionProperties.getData().getRacks().get("rack2"), is(2));
		assertThat((String)projectionProperties.getData().getProperties().get("foo"), is("bar"));

		context.close();
	}

	@Configuration
	@EnableConfigurationProperties({SpringYarnAppmasterProperties.class})
	protected static class TestConfiguration {
	}

}
