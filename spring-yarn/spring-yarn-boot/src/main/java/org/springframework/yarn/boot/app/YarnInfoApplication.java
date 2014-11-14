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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.EndpointMBeanExportAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.web.EmbeddedServletContainerAutoConfiguration;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import org.springframework.yarn.boot.SpringApplicationCallback;
import org.springframework.yarn.boot.SpringApplicationTemplate;
import org.springframework.yarn.boot.properties.SpringYarnProperties;
import org.springframework.yarn.boot.support.SpringYarnBootUtils;
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
@Configuration
@EnableAutoConfiguration(exclude = { EmbeddedServletContainerAutoConfiguration.class, WebMvcAutoConfiguration.class,
		JmxAutoConfiguration.class, BatchAutoConfiguration.class, JmxAutoConfiguration.class,
		EndpointMBeanExportAutoConfiguration.class, EndpointAutoConfiguration.class })
public class YarnInfoApplication extends AbstractClientApplication<String, YarnInfoApplication> {

	/**
	 * Run a {@link SpringApplication} build by a
	 * {@link SpringApplicationBuilder} using an empty args.
	 *
	 * @see #run(String...)
	 *
	 * @return report
	 */
	public String run() {
		return run(new String[0]);
	}

	/**
	 * Run a {@link SpringApplication} build by a {@link SpringApplicationBuilder}.
	 *
	 * @param args the Spring Application args
	 * @return report
	 */
	public String run(String... args) {
		SpringApplicationBuilder builder = new SpringApplicationBuilder();
		builder.web(false);
		builder.sources(YarnInfoApplication.class, OperationProperties.class);
		SpringYarnBootUtils.addSources(builder, sources.toArray(new Object[0]));
		SpringYarnBootUtils.addProfiles(builder, profiles.toArray(new String[0]));
		if (StringUtils.hasText(applicationBaseDir)) {
			appProperties.setProperty("spring.yarn.applicationDir", applicationBaseDir + applicationVersion + "/");
		}
		SpringYarnBootUtils.addApplicationListener(builder, appProperties);

		SpringApplicationTemplate template = new SpringApplicationTemplate(builder);
		return template.execute(new SpringApplicationCallback<String>() {

			@Override
			public String runWithSpringApplication(ApplicationContext context) throws Exception {
				OperationProperties operationProperties = context.getBean(OperationProperties.class);
				if (Operation.PUSHED == operationProperties.getOperation()) {
					return getInstalledReport(context);
				} else if (Operation.SUBMITTED == operationProperties.getOperation()) {
					YarnClient client = context.getBean(YarnClient.class);
					return getSubmittedReport(client, operationProperties.isVerbose(), operationProperties.getType(),
							operationProperties.getHeaders());
				}
				return null;
			}

		}, args);

	}

	/**
	 * Build the report for installed applications.
	 *
	 * @param context the application context
	 * @return the installed report
	 * @throws Exception the exception
	 */
	protected String getInstalledReport(ApplicationContext context) throws Exception {
		YarnConfiguration yarnConfiguration = context.getBean(YarnConfiguration.class);
		SpringYarnProperties springYarnProperties = context.getBean(SpringYarnProperties.class);

		String applicationBaseDir = springYarnProperties.getApplicationBaseDir();
		Path path = new Path(applicationBaseDir);
		FileSystem fs = path.getFileSystem(yarnConfiguration);
		FileStatus[] listStatus = new FileStatus[0];
		if (fs.exists(path)) {
			listStatus = fs.listStatus(path);
		}
		return ApplicationsReport.installedReportBuilder()
				.add(InstalledField.NAME)
				.add(InstalledField.PATH)
				.from(listStatus)
				.build().toString();
	}

	/**
	 * Build the report for submitted applications.
	 *
	 * @param client the client
	 * @param verbose the verbose
	 * @param type the type
	 * @param headers the headers
	 * @return the submitted report
	 */
	protected String getSubmittedReport(YarnClient client, boolean verbose, String type, Map<String,String> headers) {
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

	@Override
	protected YarnInfoApplication getThis() {
		return this;
	}

	@ConfigurationProperties(value = "spring.yarn.internal.YarnInfoApplication")
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
		PUSHED,
		SUBMITTED
	}

}
