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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.web.EmbeddedServletContainerAutoConfiguration;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.boot.SpringApplicationCallback;
import org.springframework.yarn.boot.SpringApplicationTemplate;
import org.springframework.yarn.boot.support.SpringYarnBootUtils;
import org.springframework.yarn.client.YarnClient;

/**
 * Generic Spring Boot client application used to install Spring Yarn Boot based apps into hdfs.
 * <p>
 * Installed application bundle is merely a collection of files inside a directory. All files
 * in this directory is considered to belong to the bundle and directory should not have any
 * other files or nested directories.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@EnableAutoConfiguration(exclude = { EmbeddedServletContainerAutoConfiguration.class, WebMvcAutoConfiguration.class,
		JmxAutoConfiguration.class, BatchAutoConfiguration.class })
public class YarnBootClientInstallApplication {

	private String appId;
	private String applicationsBaseDir;
	private List<Object> sources = new ArrayList<Object>();
	private List<String> profiles = new ArrayList<String>();
	private Map<String, Properties> configFilesContents = new HashMap<String, Properties>();
	private Properties appProperties;

	/**
	 * Sets an appid to be used by a builder.
	 *
	 * @param appId the appid
	 * @return the {@link YarnBootClientInstallApplication} for chaining
	 */
	public YarnBootClientInstallApplication appId(String appId) {
		Assert.state(StringUtils.hasText(appId), "App id must not be empty");
		this.appId = appId;
		return this;
	}

	/**
	 * Sets an Applications base directory to be used by a builder.
	 *
	 * @param applicationsBaseDir the applications base directory
	 * @return the {@link YarnBootClientInstallApplication} for chaining
	 */
	public YarnBootClientInstallApplication applicationsBaseDir(String applicationsBaseDir) {
		// can be empty because value may come from an existing properties
		this.applicationsBaseDir = applicationsBaseDir;
		return this;
	}

	/**
	 * Sets an additional sources to by used when running
	 * an {@link SpringApplication}.
	 *
	 * @param sources the additional sources for Spring Application
	 * @return the {@link YarnBootClientInstallApplication} for chaining
	 */
	public YarnBootClientInstallApplication sources(Object... sources) {
		if (!ObjectUtils.isEmpty(sources)) {
			this.sources.addAll(Arrays.asList(sources));
		}
		return this;
	}

	/**
	 * Sets an additional profiles to be used when running
	 * an {@link SpringApplication}.
	 *
	 * @param profiles the additional profiles for Spring Application
	 * @return the {@link YarnBootClientInstallApplication} for chaining
	 */
	public YarnBootClientInstallApplication profiles(String ... profiles) {
		if (!ObjectUtils.isEmpty(profiles)) {
			this.profiles.addAll(Arrays.asList(profiles));
		}
		return this;
	}

	/**
	 * Associates a new {@link Properties} with a name. These properties will
	 * be serialised into a common properties format with a given config
	 * file name.
	 *
	 * @param configFileName the config file name
	 * @param configProperties the config properties
	 * @return the {@link YarnBootClientInstallApplication} for chaining
	 */
	public YarnBootClientInstallApplication configFile(String configFileName, Properties configProperties) {
		configFilesContents.put(configFileName, configProperties);
		return this;
	}

	/**
	 * Sets application properties which will be passed into a Spring Boot
	 * environment. Properties are placed with a priority which is just below
	 * command line arguments put above all other properties. Effectively this
	 * means that these properties allow to override all existing properties
	 * but still doesn't override properties based on command line arguments.
	 *
	 * @param appProperties the app properties
	 * @return the {@link YarnBootClientInstallApplication} for chaining
	 */
	public YarnBootClientInstallApplication appProperties(Properties appProperties) {
		this.appProperties = appProperties;
		return this;
	}

	/**
	 * Run a {@link SpringApplication} build by a {@link SpringApplicationBuilder}.
	 *
	 * @param args the Spring Application args
	 */
	public void run(String... args) {
		Assert.state(StringUtils.hasText(appId), "App id must be set");

		SpringApplicationBuilder builder = new SpringApplicationBuilder();
		builder.web(false);
		builder.sources(YarnBootClientInstallApplication.class);
		SpringYarnBootUtils.addSources(builder, sources.toArray(new Object[0]));
		SpringYarnBootUtils.addProfiles(builder, profiles.toArray(new String[0]));
		SpringYarnBootUtils.addConfigFilesContents(builder, configFilesContents);
		if (StringUtils.hasText(applicationsBaseDir)) {
			if (appProperties == null) {
				appProperties = new Properties();
			}
			appProperties.setProperty("spring.yarn.applicationDir", applicationsBaseDir + appId + "/");
		}
		SpringYarnBootUtils.addApplicationListener(builder, appProperties);

		SpringApplicationTemplate template = new SpringApplicationTemplate(builder);
		template.execute(new SpringApplicationCallback<Void>() {

			@Override
			public Void runWithSpringApplication(ApplicationContext context) throws Exception {
				YarnClient client = context.getBean(YarnClient.class);
				client.installApplication();
				return null;
			}

		}, args);
	}

}
