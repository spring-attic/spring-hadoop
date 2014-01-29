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
package org.springframework.yarn.support.console;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Utility class to build reports of applications. Report can either be based
 * on states of applications in a resource manager or bundles in hdfs.
 * <p>
 * Effectively you should be able to use this class to know what applications
 * has been installed into hdfs and thus can be deployed, and if deployed what
 * are states of those. We also support various types of filters not to be
 * too verbose on a console.
 *
 * @author Janne Valkealahti
 *
 */
public class ApplicationsReport {

	private final Table table;

	/**
	 * Instantiates a new applications report.
	 * Private to be only used from a builders.
	 *
	 * @param table the rendering table
	 */
	private ApplicationsReport(Table table) {
		this.table = table;
	}

	@Override
	public String toString() {
		return table.toString();
	}

	/**
	 * Create a new builder for submitted applications.
	 *
	 * @return the report builder for submitted applications
	 */
	public static SubmittedReportBuilder submittedReportBuilder() {
		return new SubmittedReportBuilder();
	}

	/**
	 * Create a new builder for installed applications.
	 *
	 * @return the report builder for installed applications
	 */
	public static InstalledReportBuilder installedReportBuilder() {
		return new InstalledReportBuilder();
	}

	/**
	 * Builder for installed applications.
	 */
	public static class InstalledReportBuilder {

		private ArrayList<InstalledField> fields = new ArrayList<InstalledField>();

		private FileStatus[] fileStatuses;

		private InstalledReportBuilder() {
		}

		/**
		 * Adds a new field into a report.
		 *
		 * @param f the field
		 * @return the builder for chaining
		 */
		public InstalledReportBuilder add(InstalledField f) {
			fields.add(f);
			return this;
		}

		/**
		 * Add a source this report will be build from.
		 *
		 * @param fileStatuses the fileStatuses
		 * @return the builder for chaining
		 */
		public InstalledReportBuilder from(FileStatus[] fileStatuses) {
			this.fileStatuses = fileStatuses;
			return this;
		}

		/**
		 * Builds an applications report.
		 *
		 * @return the applications report
		 */
		public ApplicationsReport build() {
			Table table = new Table();
			addHeader(table);
			if (!ObjectUtils.isEmpty(fileStatuses)) {
				for (FileStatus status : fileStatuses) {
					addRow(table, status);
				}
			}
			return new ApplicationsReport(table);
		}

		private void addHeader(Table table) {
			int index = 1;
			for (InstalledField f : fields) {
				table.addHeader(index++, new TableHeader(f.getName()));
			}
		}

		private void addRow(Table table, FileStatus status) {
			final TableRow row = new TableRow();
			int index = 1;

			for (InstalledField f : fields) {
				if (InstalledField.NAME == f) {
					row.addValue(index++, status.getPath().getName());
				} else if (InstalledField.PATH == f) {
					row.addValue(index++, status.getPath().getParent().toString());
				}
			}

			table.getRows().add(row);
		}

	}

	/**
	 * Builder for submitted applications.
	 */
	public static class SubmittedReportBuilder {

		private ArrayList<SubmittedField> fields = new ArrayList<SubmittedField>();

		private SubmittedField sort;

		private List<ApplicationReport> reports;

		private Map<String, String> headerNameOverrides = new HashMap<String, String>();

		private SubmittedReportBuilder() {
		}

		/**
		 * Adds a new field into a report.
		 *
		 * @param f the field
		 * @return the builder for chaining
		 */
		public SubmittedReportBuilder add(SubmittedField f) {
			fields.add(f);
			return this;
		}

		/**
		 * Adds a new fields into a report.
		 *
		 * @param f the field
		 * @return the builder for chaining
		 */
		public SubmittedReportBuilder add(SubmittedField... f) {
			for (SubmittedField ff : f) {
				fields.add(ff);
			}
			return this;
		}

		/**
		 * Sort the report by a field.
		 *
		 * @param f the f
		 * @return the builder for chaining
		 */
		public SubmittedReportBuilder sort(SubmittedField f) {
			sort = f;
			return this;
		}

		/**
		 * Add a source this report will be build from.
		 *
		 * @param reports the reports
		 * @return the builder for chaining
		 */
		public SubmittedReportBuilder from(List<ApplicationReport> reports) {
			this.reports = reports;
			return this;
		}

		public SubmittedReportBuilder header(String from, String to) {
			headerNameOverrides.put(from.toLowerCase(), to);
			return this;
		}

		/**
		 * Builds an applications report.
		 *
		 * @return the applications report
		 */
		public ApplicationsReport build() {
			if (sort != null && reports != null) {
				Collections.sort(reports, new ApplicationReportComparator(sort));
			}
			Table table = new Table();
			addHeader(table);
			if (reports != null) {
				for (ApplicationReport report : reports) {
					addRow(table, report);
				}
			}
			return new ApplicationsReport(table);
		}

		private void addHeader(Table table) {
			int index = 1;
			for (SubmittedField f : fields) {
				table.addHeader(index++, new TableHeader(getHeaderNameMayOverride(f)));
			}
		}

		private String getHeaderNameMayOverride(SubmittedField f) {
			String n = headerNameOverrides.get(f.toString().toLowerCase());
			return StringUtils.hasText(n) ? n : f.getName();
		}

		private void addRow(Table table, ApplicationReport report) {
			final TableRow row = new TableRow();
			int index = 1;
			for (SubmittedField f : fields) {
				if (SubmittedField.ID == f) {
					row.addValue(index++, report.getApplicationId().toString());
				} else if (SubmittedField.USER == f) {
					row.addValue(index++, report.getUser());
				} else if (SubmittedField.NAME == f) {
					row.addValue(index++, report.getName());
				} else if (SubmittedField.QUEUE == f) {
					row.addValue(index++, report.getQueue());
				} else if (SubmittedField.TYPE == f) {
					row.addValue(index++, report.getApplicationType());
				} else if (SubmittedField.STARTTIME == f) {
					row.addValue(index++, DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).format(
							new Date(report.getStartTime())));
				} else if (SubmittedField.FINISHTIME == f) {
					long time = report.getFinishTime();
					String value = time > 0 ? DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).format(
							new Date(time))
							: "N/A";
					row.addValue(index++, value);
				} else if (SubmittedField.STATE == f) {
					row.addValue(index++, report.getYarnApplicationState().toString());
				} else if (SubmittedField.FINALSTATUS == f) {
					row.addValue(index++, report.getFinalApplicationStatus().toString());
				} else if (SubmittedField.ORIGTRACKURL == f) {
					String url = report.getYarnApplicationState() == YarnApplicationState.RUNNING ? report.getOriginalTrackingUrl()
							: "";
					row.addValue(index++, url);
				} else if (SubmittedField.TRACKURL == f) {
					row.addValue(index++, report.getTrackingUrl());
				}
			}
			table.getRows().add(row);
		}
	}

	/**
	 * Enums for installed applications fields.
	 */
	public static enum InstalledField {
		NAME,
		PATH;

		private String name;

		private InstalledField() {
		}

		private InstalledField(String name) {
			this.name = name;
		}

		protected String getName() {
			return StringUtils.hasText(name) ? name : this.toString();
		}
	}

	/**
	 * Enums for submitted applications fields.
	 */
	public static enum SubmittedField {
		ID("APPLICATION ID"),
		USER,
		NAME,
		QUEUE,
		TYPE,
		STARTTIME,
		FINISHTIME,
		STATE,
		FINALSTATUS,
		ORIGTRACKURL("ORIGINAL TRACKING URL"),
		TRACKURL("TRACKING URL");

		private String name;

		private SubmittedField() {
		}

		private SubmittedField(String name) {
			this.name = name;
		}

		protected String getName() {
			return StringUtils.hasText(name) ? name : this.toString();
		}
	}

	/**
	 * Sort comparator for {@code ApplicationReport}.
	 */
	private static class ApplicationReportComparator implements Comparator<ApplicationReport> {

		private final SubmittedField f;

		private ApplicationReportComparator(SubmittedField f) {
			this.f = f;
		}

		@Override
		public int compare(ApplicationReport l, ApplicationReport r) {
			if (SubmittedField.ID == f) {
				return -(l.getApplicationId().toString().compareTo(r.getApplicationId().toString()));
			}
			else if (SubmittedField.USER == f) {
				return l.getUser().compareTo(r.getUser());
			}
			else {
				return 0;
			}
		}

	}

}
