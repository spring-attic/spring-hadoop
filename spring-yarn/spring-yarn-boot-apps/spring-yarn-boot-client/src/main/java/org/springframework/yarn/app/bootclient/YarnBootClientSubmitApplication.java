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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.web.EmbeddedServletContainerAutoConfiguration;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.yarn.boot.support.SpringYarnBootUtils;
import org.springframework.yarn.client.YarnClient;

/**
 * Generic Spring Boot client application used to submit Spring Yarn Boot based apps into Yarn.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@EnableAutoConfiguration(exclude = { EmbeddedServletContainerAutoConfiguration.class, WebMvcAutoConfiguration.class,
		JmxAutoConfiguration.class, BatchAutoConfiguration.class })
public class YarnBootClientSubmitApplication {

	private static final Log log = LogFactory.getLog(YarnBootClientSubmitApplication.class);

	public ApplicationId submit(String[] profiles, org.apache.hadoop.conf.Configuration configuration, String[] args) {
		Properties props = new Properties();

		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "fs.defaultFS", "spring.yarn.fsUri", props);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.address", "spring.yarn.rmAddress", props);
		SpringYarnBootUtils.mergeHadoopPropertyIntoMap(configuration, "yarn.resourcemanager.scheduler.address", "spring.yarn.schedulerAddress", props);
		SpringYarnBootUtils.mergeBootArgumentsIntoMap(args, props);
		SpringYarnBootUtils.appendAsCommaDelimitedIntoProperties("spring.profiles.active", profiles, props);

		return run(SpringYarnBootUtils.propertiesToBootArguments(props));
	}

	public ApplicationId run(String... args) {
		ApplicationId applicationId = null;
		ConfigurableApplicationContext context = null;
		Exception exception = null;

		try {
			context = new SpringApplicationBuilder(YarnBootClientSubmitApplication.class)
					.web(false)
					.run(args);

			YarnClient client = context.getBean(YarnClient.class);
			applicationId = client.submitApplication(false);
		}
		catch (Exception e) {
			exception = e;
			log.debug("Error submitting new application instance", e);
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
			throw new RuntimeException("Failed to submit application instance, " + exception.getMessage(), exception);
		}
		return applicationId;
	}

	public static void main(String[] args) {
		new YarnBootClientSubmitApplication().run(args);
	}

}
