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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.util.StringUtils;

/**
 * Utility class creating reports for container clusters.
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerClusterReport {

	private final Table table;

	private ContainerClusterReport(Table table) {
		this.table = table;
	}

	@Override
	public String toString() {
		return table.toString();
	}

	public static ClustersInfoReportBuilder clustersInfoReportBuilder() {
		return new ClustersInfoReportBuilder();
	}

	public static ClusterInfoReportBuilder clusterInfoReportBuilder() {
		return new ClusterInfoReportBuilder();
	}

	public static class ClustersInfoReportBuilder {

		private ArrayList<ClustersInfoField> fields = new ArrayList<ClustersInfoField>();

		private List<String> reports;

		private Map<String, String> headerNameOverrides = new HashMap<String, String>();

		private ClustersInfoReportBuilder() {
		}

		/**
		 * Adds a new field into a report.
		 *
		 * @param f the field
		 * @return the builder for chaining
		 */
		public ClustersInfoReportBuilder add(ClustersInfoField f) {
			fields.add(f);
			return this;
		}

		/**
		 * Adds a new fields into a report.
		 *
		 * @param f the field
		 * @return the builder for chaining
		 */
		public ClustersInfoReportBuilder add(ClustersInfoField... f) {
			for (ClustersInfoField ff : f) {
				fields.add(ff);
			}
			return this;
		}

		/**
		 * Add a source this report will be build from.
		 *
		 * @param reports the reports
		 * @return the builder for chaining
		 */
		public ClustersInfoReportBuilder from(List<String> reports) {
			this.reports = reports;
			return this;
		}

		public ClustersInfoReportBuilder header(String from, String to) {
			headerNameOverrides.put(from.toLowerCase(), to);
			return this;
		}

		/**
		 * Builds an applications report.
		 *
		 * @return the applications report
		 */
		public ContainerClusterReport build() {
			Table table = new Table();
			addHeader(table);
			if (reports != null) {
				for (String report : reports) {
					addRow(table, report);
				}
			}
			return new ContainerClusterReport(table);
		}

		private void addHeader(Table table) {
			int index = 1;
			for (ClustersInfoField f : fields) {
				table.addHeader(index++, new TableHeader(getHeaderNameMayOverride(f)));
			}
		}

		private String getHeaderNameMayOverride(ClustersInfoField f) {
			String n = headerNameOverrides.get(f.toString().toLowerCase());
			return StringUtils.hasText(n) ? n : f.getName();
		}

		private void addRow(Table table, String report) {
			final TableRow row = new TableRow();
			int index = 1;
			for (ClustersInfoField f : fields) {
				if (ClustersInfoField.ID == f) {
					row.addValue(index++, report);
				}
			}
			table.getRows().add(row);
		}
	}

	public static class ClusterInfoReportBuilder {

		private ArrayList<ClusterInfoField> fields = new ArrayList<ClusterInfoField>();

		private List<ClustersInfoReportData> reports;

		private Map<String, String> headerNameOverrides = new HashMap<String, String>();

		private ClusterInfoReportBuilder() {
		}

		/**
		 * Adds a new field into a report.
		 *
		 * @param f the field
		 * @return the builder for chaining
		 */
		public ClusterInfoReportBuilder add(ClusterInfoField f) {
			fields.add(f);
			return this;
		}

		/**
		 * Adds a new fields into a report.
		 *
		 * @param f the field
		 * @return the builder for chaining
		 */
		public ClusterInfoReportBuilder add(ClusterInfoField... f) {
			for (ClusterInfoField ff : f) {
				fields.add(ff);
			}
			return this;
		}

		/**
		 * Add a source this report will be build from.
		 *
		 * @param reports the reports
		 * @return the builder for chaining
		 */
		public ClusterInfoReportBuilder from(List<ClustersInfoReportData> reports) {
			this.reports = reports;
			return this;
		}

		public ClusterInfoReportBuilder header(String from, String to) {
			headerNameOverrides.put(from.toLowerCase(), to);
			return this;
		}

		/**
		 * Builds an applications report.
		 *
		 * @return the applications report
		 */
		public ContainerClusterReport build() {
			Table table = new Table();
			addHeader(table);
			if (reports != null) {
				for (ClustersInfoReportData report : reports) {
					addRow(table, report);
				}
			}
			return new ContainerClusterReport(table);
		}

		private void addHeader(Table table) {
			int index = 1;
			for (ClusterInfoField f : fields) {
				table.addHeader(index++, new TableHeader(getHeaderNameMayOverride(f)));
			}
		}

		private String getHeaderNameMayOverride(ClusterInfoField f) {
			String n = headerNameOverrides.get(f.toString().toLowerCase());
			return StringUtils.hasText(n) ? n : f.getName();
		}

		private void addRow(Table table, ClustersInfoReportData report) {
			final TableRow row = new TableRow();
			int index = 1;
			for (ClusterInfoField f : fields) {
				if (ClusterInfoField.STATE == f) {
					row.addValue(index++, report.getState());
				} else if (ClusterInfoField.MEMBERS == f) {
					row.addValue(index++, report.getCount() != null ? report.getCount().toString() : "N/A");
				} else if (ClusterInfoField.PROJECTIONANY == f) {
					row.addValue(index++, report.getProjectionAny() != null ? report.getProjectionAny().toString() : "N/A");
				} else if (ClusterInfoField.PROJECTIONHOSTS == f) {
					row.addValue(index++, report.getProjectionHosts() != null ? report.getProjectionHosts().toString() : "N/A");
				} else if (ClusterInfoField.PROJECTIONRACKS == f) {
					row.addValue(index++, report.getProjectionRacks() != null ? report.getProjectionRacks().toString() : "N/A");
				} else if (ClusterInfoField.SATISFYANY == f) {
					row.addValue(index++, report.getSatisfyAny() != null ? report.getSatisfyAny().toString() : "N/A");
				} else if (ClusterInfoField.SATISFYHOSTS == f) {
					row.addValue(index++, report.getSatisfyHosts() != null ? report.getSatisfyHosts().toString() : "N/A");
				} else if (ClusterInfoField.SATISFYRACKS == f) {
					row.addValue(index++, report.getSatisfyRacks() != null ? report.getSatisfyRacks().toString() : "N/A");
				}
			}
			table.getRows().add(row);
		}
	}

	/**
	 * Enums for clusters fields.
	 */
	public static enum ClustersInfoField {
		ID("CLUSTER ID");

		private String name;

		private ClustersInfoField() {
		}

		private ClustersInfoField(String name) {
			this.name = name;
		}

		protected String getName() {
			return StringUtils.hasText(name) ? name : this.toString();
		}
	}

	/**
	 * Enums for cluster fields.
	 */
	public static enum ClusterInfoField {
		STATE("CLUSTER STATE"),
		MEMBERS("MEMBER COUNT"),
		PROJECTIONANY("ANY PROJECTION"),
		PROJECTIONHOSTS("HOSTS PROJECTION"),
		PROJECTIONRACKS("RACKS PROJECTION"),
		SATISFYANY("ANY SATISFY"),
		SATISFYHOSTS("HOSTS SATISFY"),
		SATISFYRACKS("RACKS SATISFY");

		private String name;

		private ClusterInfoField() {
		}

		private ClusterInfoField(String name) {
			this.name = name;
		}

		protected String getName() {
			return StringUtils.hasText(name) ? name : this.toString();
		}
	}

	public static class ClustersInfoReportData {
		private String state;
		private Integer count;
		private Integer pany;
		private Map<String, Integer> phosts;
		private Map<String, Integer> pracks;
		private Integer sany;
		private Map<String, Integer> shosts;
		private Map<String, Integer> sracks;

		public ClustersInfoReportData(String state, Integer count, Integer pany, Map<String, Integer> phosts,
				Map<String, Integer> pracks, Integer sany, Map<String, Integer> shosts, Map<String, Integer> sracks) {
			this.state = state;
			this.count = count;
			this.pany = pany;
			this.phosts = phosts;
			this.pracks = pracks;
			this.sany = sany;
			this.shosts = shosts;
			this.sracks = sracks;
		}
		public String getState() {
			return state;
		}
		public Integer getCount() {
			return count;
		}
		public Integer getProjectionAny() {
			return pany;
		}
		public Map<String, Integer> getProjectionHosts() {
			return phosts;
		}
		public Map<String, Integer> getProjectionRacks() {
			return pracks;
		}
		public Integer getSatisfyAny() {
			return sany;
		}
		public Map<String, Integer> getSatisfyHosts() {
			return shosts;
		}
		public Map<String, Integer> getSatisfyRacks() {
			return sracks;
		}
	}

}
