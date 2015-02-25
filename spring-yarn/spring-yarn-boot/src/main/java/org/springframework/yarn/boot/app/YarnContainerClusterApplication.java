/*
 * Copyright 2014-2015 the original author or authors.
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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.util.ConverterUtils;
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
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.cluster.ContainerCluster;
import org.springframework.yarn.boot.SpringApplicationCallback;
import org.springframework.yarn.boot.SpringApplicationTemplate;
import org.springframework.yarn.boot.actuate.endpoint.YarnContainerClusterEndpoint;
import org.springframework.yarn.boot.actuate.endpoint.mvc.AbstractContainerClusterRequest.ProjectionDataType;
import org.springframework.yarn.boot.actuate.endpoint.mvc.ContainerClusterCreateRequest;
import org.springframework.yarn.boot.actuate.endpoint.mvc.ContainerClusterModifyRequest;
import org.springframework.yarn.boot.actuate.endpoint.mvc.YarnContainerClusterMvcEndpoint;
import org.springframework.yarn.boot.actuate.endpoint.mvc.domain.ContainerClusterResource;
import org.springframework.yarn.boot.actuate.endpoint.mvc.domain.YarnContainerClusterEndpointResource;
import org.springframework.yarn.boot.support.SpringYarnBootUtils;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.support.console.ContainerClusterReport;
import org.springframework.yarn.support.console.ContainerClusterReport.ClusterInfoField;
import org.springframework.yarn.support.console.ContainerClusterReport.ClustersInfoField;
import org.springframework.yarn.support.console.ContainerClusterReport.ClustersInfoReportData;

/**
 * A Boot application which is used to control Spring YARN {@link ContainerCluster}s
 * via rest API offered by a {@link YarnContainerClusterMvcEndpoint}.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@EnableAutoConfiguration(exclude = { EmbeddedServletContainerAutoConfiguration.class, WebMvcAutoConfiguration.class,
		JmxAutoConfiguration.class, BatchAutoConfiguration.class, JmxAutoConfiguration.class,
		EndpointMBeanExportAutoConfiguration.class, EndpointAutoConfiguration.class })
public class YarnContainerClusterApplication extends AbstractClientApplication<String, YarnContainerClusterApplication> {

	@Override
	protected YarnContainerClusterApplication getThis() {
		return this;
	}

	public String run() {
		return run(new String[0]);
	}

	public String run(String... args) {
		SpringApplicationBuilder builder = new SpringApplicationBuilder();
		builder.web(false);
		builder.sources(YarnContainerClusterApplication.class, OperationProperties.class);
		SpringYarnBootUtils.addSources(builder, sources.toArray(new Object[0]));
		SpringYarnBootUtils.addProfiles(builder, profiles.toArray(new String[0]));
		SpringYarnBootUtils.addApplicationListener(builder, appProperties);

		SpringApplicationTemplate template = new SpringApplicationTemplate(builder);
		return template.execute(new SpringApplicationCallback<String>() {

			@Override
			public String runWithSpringApplication(ApplicationContext context) throws Exception {
				OperationProperties operationProperties = context.getBean(OperationProperties.class);
				YarnClient client = context.getBean(YarnClient.class);
				RestTemplate restTemplate = context.getBean(YarnSystemConstants.DEFAULT_ID_RESTTEMPLATE,
						RestTemplate.class);
				ApplicationId applicationId = ConverterUtils.toApplicationId(operationProperties.getApplicationId());
				String clusterId = operationProperties.getClusterId();
				String clusterDef = operationProperties.getClusterDef();
				String projectionType = operationProperties.getProjectionType();
				ProjectionDataProperties projectionData = operationProperties.getProjectionData();
				Integer projectionDataAny = projectionData != null ? projectionData.getAny() : null;
				Map<String, Integer> projectionDataHosts = projectionData != null ? projectionData.getHosts() : null;
				Map<String, Integer> projectionDataRacks = projectionData != null ? projectionData.getRacks() : null;
				Map<String, Object> projectionDataProperties = projectionData != null ? projectionData.getProperties() : null;
				Map<String, Object> extraProperties = operationProperties.getExtraProperties();
				Operation operation = operationProperties.getOperation();
				boolean verbose = operationProperties.isVerbose();

				if (Operation.CLUSTERSINFO == operation) {
					return doClustersInfo(restTemplate, client, applicationId);
				} else if (Operation.CLUSTERINFO == operation) {
					return doClusterInfo(restTemplate, client, applicationId, clusterId, verbose);
				} else if (Operation.CLUSTERCREATE == operation) {
					return doClusterCreate(restTemplate, client, applicationId, clusterId, clusterDef, projectionType,
							projectionDataAny, projectionDataHosts, projectionDataRacks, projectionDataProperties,
							extraProperties);
				} else if (Operation.CLUSTERDESTROY == operation) {
					return doClusterDestroy(restTemplate, client, applicationId, clusterId);
				} else if (Operation.CLUSTERMODIFY == operation) {
					return doClusterModify(restTemplate, client, applicationId, clusterId, projectionDataAny, projectionDataHosts,
							projectionDataRacks, projectionDataProperties);
				} else if (Operation.CLUSTERSTART == operation) {
					return doClusterStart(restTemplate, client, applicationId, clusterId);
				} else if (Operation.CLUSTERSTOP == operation) {
					return doClusterStop(restTemplate, client, applicationId, clusterId);
				}
				return null;
			}

		}, args);

	}

	private String doClustersInfo(RestTemplate restTemplate, YarnClient client, ApplicationId applicationId) {
		YarnContainerClusterOperations operations = buildClusterOperations(restTemplate, client, applicationId);
		YarnContainerClusterEndpointResource response = operations.getClusters();
		return ContainerClusterReport.clustersInfoReportBuilder()
				.add(ClustersInfoField.ID)
				.from(new ArrayList<String>(response.getClusters()))
				.build().toString();
	}

	private String doClusterInfo(RestTemplate restTemplate, YarnClient client, ApplicationId applicationId, String clusterId, boolean verbose) {
		YarnContainerClusterOperations operations = buildClusterOperations(restTemplate, client, applicationId);
		ContainerClusterResource response = operations.clusterInfo(clusterId);

		List<ClustersInfoReportData> data = new ArrayList<ClustersInfoReportData>();

		Integer pany = response.getGridProjection().getProjectionData().getAny();
		Map<String, Integer> phosts = response.getGridProjection().getProjectionData().getHosts();
		Map<String, Integer> pracks = response.getGridProjection().getProjectionData().getRacks();
		Integer sany = response.getGridProjection().getSatisfyState().getAllocateData().getAny();
		Map<String, Integer> shosts = response.getGridProjection().getSatisfyState().getAllocateData().getHosts();
		Map<String, Integer> sracks = response.getGridProjection().getSatisfyState().getAllocateData().getRacks();

		data.add(new ClustersInfoReportData(response.getContainerClusterState().getClusterState().toString(), response
				.getGridProjection().getMembers().size(), pany, phosts, pracks, sany, shosts, sracks));
		if (verbose) {
			return ContainerClusterReport.clusterInfoReportBuilder()
					.add(ClusterInfoField.STATE)
					.add(ClusterInfoField.MEMBERS)
					.add(ClusterInfoField.PROJECTIONANY)
					.add(ClusterInfoField.PROJECTIONHOSTS)
					.add(ClusterInfoField.PROJECTIONRACKS)
					.add(ClusterInfoField.SATISFYANY)
					.add(ClusterInfoField.SATISFYHOSTS)
					.add(ClusterInfoField.SATISFYRACKS)
					.from(data)
					.build().toString();
		} else {
			return ContainerClusterReport.clusterInfoReportBuilder()
					.add(ClusterInfoField.STATE)
					.add(ClusterInfoField.MEMBERS)
					.from(data)
					.build().toString();
		}
	}

	private String doClusterCreate(RestTemplate restTemplate, YarnClient client, ApplicationId applicationId, String clusterId, String clusterDef,
			String projectionType, Integer projectionDataAny, Map<String, Integer> hosts, Map<String, Integer> racks,
			Map<String, Object> projectionDataProperties, Map<String, Object> extraProperties) {
		YarnContainerClusterOperations operations = buildClusterOperations(restTemplate, client, applicationId);

		ContainerClusterCreateRequest request = new ContainerClusterCreateRequest();
		request.setClusterId(clusterId);
		request.setClusterDef(clusterDef);
		request.setProjection(projectionType);
		request.setExtraProperties(extraProperties);

		ProjectionDataType projectionData = new ProjectionDataType();
		projectionData.setAny(projectionDataAny);
		projectionData.setHosts(hosts);
		projectionData.setRacks(racks);
		projectionData.setProperties(projectionDataProperties);

		request.setProjectionData(projectionData);
		operations.clusterCreate(request);
		return "Cluster " + clusterId + " created.";
	}

	private String doClusterDestroy(RestTemplate restTemplate, YarnClient client, ApplicationId applicationId, String clusterId) {
		YarnContainerClusterOperations operations = buildClusterOperations(restTemplate, client, applicationId);
		operations.clusterDestroy(clusterId);
		return "Cluster " + clusterId + " destroyed.";
	}

	private String doClusterStart(RestTemplate restTemplate, YarnClient client, ApplicationId applicationId, String clusterId) {
		YarnContainerClusterOperations operations = buildClusterOperations(restTemplate, client, applicationId);
		ContainerClusterModifyRequest request = new ContainerClusterModifyRequest();
		request.setAction("start");
		operations.clusterStart(clusterId, request);
		return "Cluster " + clusterId + " started.";
	}

	private String doClusterStop(RestTemplate restTemplate, YarnClient client, ApplicationId applicationId, String clusterId) {
		YarnContainerClusterOperations operations = buildClusterOperations(restTemplate, client, applicationId);
		ContainerClusterModifyRequest request = new ContainerClusterModifyRequest();
		request.setAction("stop");
		operations.clusterStop(clusterId, request);
		return "Cluster " + clusterId + " stopped.";
	}

	private String doClusterModify(RestTemplate restTemplate, YarnClient client, ApplicationId applicationId, String clusterId,
			Integer projectionDataAny, Map<String, Integer> hosts, Map<String, Integer> racks,
			Map<String, Object> properties) {
		YarnContainerClusterOperations operations = buildClusterOperations(restTemplate, client, applicationId);

		ContainerClusterCreateRequest request = new ContainerClusterCreateRequest();
		request.setClusterId(clusterId);

		ProjectionDataType projectionData = new ProjectionDataType();
		projectionData.setAny(projectionDataAny);
		projectionData.setHosts(hosts);
		projectionData.setRacks(racks);
		projectionData.setProperties(properties);

		request.setProjectionData(projectionData);

		operations.clusterModify(clusterId, request);
		return "Cluster " + clusterId + " modified.";
	}

	private YarnContainerClusterOperations buildClusterOperations(RestTemplate restTemplate, YarnClient client, ApplicationId applicationId) {
		ApplicationReport report = client.getApplicationReport(applicationId);
		String trackingUrl = report.getOriginalTrackingUrl();
		return new YarnContainerClusterTemplate(trackingUrl + "/" + YarnContainerClusterEndpoint.ENDPOINT_ID, restTemplate);
	}

	@ConfigurationProperties(value = "spring.yarn.internal.ContainerClusterApplication")
	public static class OperationProperties {

		private Operation operation;

		private String applicationId;

		private String clusterId;

		private String clusterDef;

		private String projectionType;

		private ProjectionDataProperties projectionData;

		private Map<String, Object> extraProperties;

		private boolean verbose;

		public void setOperation(Operation operation) {
			this.operation = operation;
		}

		public Operation getOperation() {
			return operation;
		}

		public void setApplicationId(String applicationId) {
			this.applicationId = applicationId;
		}

		public String getApplicationId() {
			return applicationId;
		}

		public void setClusterId(String clusterId) {
			this.clusterId = clusterId;
		}

		public String getClusterId() {
			return clusterId;
		}

		public void setClusterDef(String clusterDef) {
			this.clusterDef = clusterDef;
		}

		public String getClusterDef() {
			return clusterDef;
		}

		public void setProjectionType(String projectionType) {
			this.projectionType = projectionType;
		}

		public String getProjectionType() {
			return projectionType;
		}

		public ProjectionDataProperties getProjectionData() {
			return projectionData;
		}

		public void setProjectionData(ProjectionDataProperties projectionData) {
			this.projectionData = projectionData;
		}

		public void setExtraProperties(Map<String, Object> extraProperties) {
			this.extraProperties = extraProperties;
		}

		public Map<String, Object> getExtraProperties() {
			return extraProperties;
		}

		public void setVerbose(boolean verbose) {
			this.verbose = verbose;
		}

		public boolean isVerbose() {
			return verbose;
		}

	}

	public static class ProjectionDataProperties {

		private Integer any;

		private Map<String, Integer> hosts;

		private Map<String, Integer> racks;

		private Map<String, Object> properties;

		public Integer getAny() {
			return any;
		}

		public void setAny(Integer any) {
			this.any = any;
		}

		public Map<String, Integer> getHosts() {
			return hosts;
		}

		public void setHosts(Map<String, Integer> hosts) {
			this.hosts = hosts;
		}

		public Map<String, Integer> getRacks() {
			return racks;
		}

		public void setRacks(Map<String, Integer> racks) {
			this.racks = racks;
		}

		public Map<String, Object> getProperties() {
			return properties;
		}

		public void setProperties(Map<String, Object> properties) {
			this.properties = properties;
		}

	}

	/**
	 * Operations supported by this application.
	 */
	private enum Operation {
		CLUSTERSINFO,
		CLUSTERINFO,
		CLUSTERCREATE,
		CLUSTERDESTROY,
		CLUSTERMODIFY,
		CLUSTERSTART,
		CLUSTERSTOP
	}

}
