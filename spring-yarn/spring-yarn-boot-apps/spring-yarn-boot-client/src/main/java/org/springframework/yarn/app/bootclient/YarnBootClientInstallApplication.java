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
package org.springframework.yarn.app.bootclient;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.web.EmbeddedServletContainerAutoConfiguration;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;
import org.springframework.yarn.boot.support.SpringYarnBootUtils;
import org.springframework.yarn.boot.support.SpringYarnProperties;
import org.springframework.yarn.client.YarnClient;

/**
 * Generic Spring Boot client application used to install Spring Yarn Boot based apps into hdfs.
 * <p>
 * Installed application bundle is merely a collection of files inside a directory. All files
 * in this directory is considered to belong to the bundle and directory should not have any
 * other files or sub-directories.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@EnableAutoConfiguration(exclude = { EmbeddedServletContainerAutoConfiguration.class, WebMvcAutoConfiguration.class,
		JmxAutoConfiguration.class, BatchAutoConfiguration.class })
public class YarnBootClientInstallApplication {

	private static final Log log = LogFactory.getLog(YarnBootClientInstallApplication.class);

	/**
	 * Install new application bundle into hdfs.
	 *
	 * @param id the unique identifier of the bundle in hdfs
	 * @param profiles the additional spring profiles
	 * @param properties the properties
	 * @param configuration the hadoop configuration
	 * @param args the additional Spring Boot run args
	 */
	public void install(String id, String[] profiles, Properties properties, org.apache.hadoop.conf.Configuration configuration, String[] args) {
		Properties props = new Properties();

		// merge settings set by user in a shell
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "fs.defaultFS", "spring.yarn.fsUri", props);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.address", "spring.yarn.rmAddress", props);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.scheduler.address", "spring.yarn.schedulerAddress", props);
		SpringYarnBootUtils.mergeBootArgumentsIntoMap(args, props);
		CollectionUtils.mergePropertiesIntoMap(properties, props);
		SpringYarnBootUtils.appendAsCommaDelimitedIntoProperties("spring.profiles.active", profiles, props);

		// based on a given id, set application dir and shared
		// applications base dir.
		String applicationsBaseDir = props.getProperty("spring.yarn.applicationsBaseDir");
		if (applicationsBaseDir != null) {
			props.setProperty("spring.yarn.applicationDir", applicationsBaseDir + id + "/");
		}

		// convert all properties and settings into boot args and run the application
		run(SpringYarnBootUtils.propertiesToBootArguments(props));
	}

	/**
	 * Run application and use {@code YarnClient} to initiate
	 * an application bundle installation process.
	 *
	 * @param args the Spring Boot run args
	 */
	public void run(String... args) {
		ConfigurableApplicationContext context = null;
		Exception exception = null;

		try {
			context = new SpringApplicationBuilder(YarnBootClientInstallApplication.class)
					.web(false)
					.run(args);

			YarnClient client = context.getBean(YarnClient.class);
			YarnConfiguration yarnConfiguration = context.getBean(YarnConfiguration.class);

			SpringYarnProperties springYarnProperties = context.getBean(SpringYarnProperties.class);

			if (springYarnProperties != null) {
				String applicationDir = springYarnProperties.getApplicationDir();
				FileSystem fs = FileSystem.get(yarnConfiguration);
				FSDataOutputStream out = fs.create(new Path(applicationDir, "application.properties"));
				for (String arg : args) {
					if (arg.startsWith("--")) {
						out.writeBytes(arg.substring(2) + "\n");
					}
				}
				out.close();
			}

			client.installApplication();
		}
		catch (Exception e) {
			exception = e;
			log.debug("Error installing new application instance", e);
		}
		finally {
			if (context != null) {
				try {
					context.close();
				}
				catch (Exception e) {
					log.debug("Error closing context", e);
				}
				context = null;
			}
		}

		if (exception != null) {
			throw new RuntimeException("Failed to install application instance, " + exception.getMessage(), exception);
		}
	}

	public static void main(String[] args) {
		new YarnBootClientInstallApplication().run(args);
	}

}
