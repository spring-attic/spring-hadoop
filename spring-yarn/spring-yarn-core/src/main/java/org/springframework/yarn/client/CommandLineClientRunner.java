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
package org.springframework.yarn.client;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.springframework.util.StringUtils;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.launch.AbstractCommandLineRunner;
import org.springframework.yarn.launch.ExitStatus;
import org.springframework.yarn.support.console.Table;
import org.springframework.yarn.support.console.TableHeader;
import org.springframework.yarn.support.console.TableRow;
import org.springframework.yarn.support.console.UiUtils;

/**
 * A simple client runner executing a bean named "yarnClient".
 *
 * @author Janne Valkealahti
 *
 */
public class CommandLineClientRunner extends AbstractCommandLineRunner<YarnClient> {

	public final static String OPT_SUBMIT = "-submit";
	public final static String OPT_KILL = "-kill";
	public final static String OPT_LIST = "-list";
	public final static String ARG_APPLICATION_ID = "applicationId";

	@SuppressWarnings("serial")
	private final static List<String> opts = new ArrayList<String>() {{
	    add(OPT_SUBMIT);
	    add(OPT_KILL);
	    add(OPT_LIST);
	}};

	@Override
	protected ExitStatus handleBeanRun(YarnClient bean, String[] parameters, Set<String> opts) {
		Properties properties = StringUtils.splitArrayElementsIntoProperties(parameters, "=");

		if (opts.contains(OPT_SUBMIT)) {
			bean.submitApplication();
		} else if (opts.contains(OPT_LIST)) {
			printApplicationsReport(bean.listApplications());
		} else if (opts.contains(OPT_KILL)) {
			ApplicationId appId = queryApplicationId(bean,
					properties != null ? properties.getProperty(ARG_APPLICATION_ID) : null);
			if (appId != null) {
				bean.killApplication(appId);
			}
		}

		return ExitStatus.COMPLETED;
	}

	@Override
	protected String getDefaultBeanIdentifier() {
		return YarnSystemConstants.DEFAULT_ID_CLIENT;
	}

	@Override
	protected List<String> getValidOpts() {
		return opts;
	}

	/**
	 * Query application id.
	 *
	 * @param client the yarn client
	 * @param applicationId the application id
	 * @return the application id if exists, NULL otherwise
	 */
	protected ApplicationId queryApplicationId(YarnClient client, String applicationId) {
		if (!StringUtils.hasText(applicationId)) {
			return null;
		}
		ApplicationId appId = null;
		for (ApplicationReport a : client.listApplications()) {
			if (a.getApplicationId().toString().equals(applicationId)) {
				appId = a.getApplicationId();
				break;
			}
		}
		return appId;
	}

	/**
	 * Prints the applications report into system out.
	 *
	 * @param applications the applications
	 */
	private static void printApplicationsReport(List<ApplicationReport> applications) {
		System.out.println(UiUtils.renderTextTable(getApplicationReportTable(applications), true));
	}

	/**
	 * Gets the application report table.
	 *
	 * @param applications the applications
	 * @return the application report table
	 */
	private static Table getApplicationReportTable(List<ApplicationReport> applications) {
		Table table = new Table();
		table.addHeader(1, new TableHeader("Id"))
				.addHeader(2, new TableHeader("User"))
				.addHeader(3, new TableHeader("Name"))
				.addHeader(4, new TableHeader("Queue"))
				.addHeader(5, new TableHeader("StartTime"))
				.addHeader(6, new TableHeader("FinishTime"))
				.addHeader(7, new TableHeader("State"))
				.addHeader(8, new TableHeader("FinalStatus"));

		for (ApplicationReport a : applications) {
			final TableRow row = new TableRow();
			row.addValue(1, a.getApplicationId().toString())
					.addValue(2, a.getUser())
					.addValue(3, a.getName())
					.addValue(4, a.getQueue())
					.addValue(5, DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).format(
							new Date(a.getStartTime())))
					.addValue(6, DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).format(
							new Date(a.getFinishTime())))
					.addValue(7, a.getYarnApplicationState().toString())
					.addValue(8, a.getFinalApplicationStatus().toString());
			table.getRows().add(row);
		}
		return table;
	}

	public static void main(String[] args) {
		new CommandLineClientRunner().doMain(args);
	}

}
