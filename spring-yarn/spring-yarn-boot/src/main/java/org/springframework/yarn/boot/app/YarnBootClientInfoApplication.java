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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.CollectionUtils;
import org.springframework.yarn.boot.support.SpringYarnBootUtils;
import org.springframework.yarn.boot.support.SpringYarnProperties;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.support.console.ApplicationsReport;
import org.springframework.yarn.support.console.ApplicationsReport.InstalledField;
import org.springframework.yarn.support.console.ApplicationsReport.SubmittedField;
import org.springframework.yarn.support.console.ApplicationsReport.SubmittedReportBuilder;

/**
 * Generic Spring Yarn Boot application handling reporting.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnBootClientInfoApplication {

	private static final Log log = LogFactory.getLog(YarnBootClientInfoApplication.class);

	public String info(String id, String[] profiles, Properties properties, org.apache.hadoop.conf.Configuration configuration, String[] args) {
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
		return run(SpringYarnBootUtils.propertiesToBootArguments(props));
	}

	public String run(String... args) {
		ConfigurableApplicationContext context = null;
		Exception exception = null;
		String info = null;

		try {
			context = new SpringApplicationBuilder(YarnBootClientInstallApplication.class, OperationProperties.class)
					.web(false)
					.run(args);
			OperationProperties operationProperties = context.getBean(OperationProperties.class);

			if (Operation.INSTALLED == operationProperties.getOperation()) {
				info = getInstalledReport(context);
			} else if (Operation.SUBMITTED == operationProperties.getOperation()) {
				info = getSubmittedReport(context, operationProperties.isVerbose(), operationProperties.getType(), operationProperties.getHeaders());
			} else {
				// it's ok to fail fast, these properties are pretty much used internally
				throw new IllegalArgumentException("Operation not found");
			}

		}
		catch (Exception e) {
			exception = e;
			log.debug("Error running reporting", e);
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
			throw new RuntimeException("Failed to run reporting, " + exception.getMessage(), exception);
		}
		return info;
	}

	private String getInstalledReport(ApplicationContext context) throws Exception {
		YarnConfiguration yarnConfiguration = context.getBean(YarnConfiguration.class);
		SpringYarnProperties springYarnProperties = context.getBean(SpringYarnProperties.class);

		String applicationsBaseDir = springYarnProperties.getApplicationsBaseDir();
		Path path = new Path(applicationsBaseDir);
		FileSystem fs = path.getFileSystem(yarnConfiguration);
		FileStatus[] listStatus = fs.listStatus(path);

		return ApplicationsReport.installedReportBuilder()
				.add(InstalledField.NAME)
				.add(InstalledField.PATH)
				.from(listStatus)
				.build().toString();
	}

	private String getSubmittedReport(ApplicationContext context, boolean verbose, String type, Map<String,String> headers) {
		YarnClient client = context.getBean(YarnClient.class);
		SubmittedReportBuilder builder = ApplicationsReport.submittedReportBuilder();
		builder.add(SubmittedField.ID, SubmittedField.USER, SubmittedField.NAME, SubmittedField.QUEUE,
				SubmittedField.TYPE, SubmittedField.STARTTIME, SubmittedField.FINISHTIME, SubmittedField.STATE,
				SubmittedField.FINALSTATUS, SubmittedField.ORIGTRACKURL)
				.sort(SubmittedField.ID);
		if (verbose) {
			builder.from(client.listApplications(type));
		} else {
			builder.from(client.listRunningApplications(type));
		}
		if (headers != null) {
			for (Entry<String, String> entry : headers.entrySet()) {
				builder.header(entry.getKey(), entry.getValue());
			}
		}
		return builder.build().toString();
	}

	public static void main(String[] args) {
		new YarnBootClientInfoApplication().run(args);
	}

	@ConfigurationProperties(name = "spring.yarn.internal.YarnBootClientInfoApplication")
	public static class OperationProperties {
		private Operation operation;
		private boolean verbose;
		private String type;
		private Map<String, String> headers;
		public void setOperation(Operation operation) {
			this.operation = operation;
		}
		public Operation getOperation() {
			return operation;
		}
		public void setVerbose(boolean verbose) {
			this.verbose = verbose;
		}
		public boolean isVerbose() {
			return verbose;
		}
		public void setType(String type) {
			this.type = type;
		}
		public String getType() {
			return type;
		}
		public void setHeaders(Map<String, String> headers) {
			this.headers = headers;
		}
		public Map<String, String> getHeaders() {
			return headers;
		}
	}

	public static enum Operation {
		INSTALLED,
		SUBMITTED
	}

}
