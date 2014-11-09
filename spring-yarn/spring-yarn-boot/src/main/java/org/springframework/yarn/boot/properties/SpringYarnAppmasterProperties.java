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
package org.springframework.yarn.boot.properties;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.yarn.am.grid.support.DefaultGridProjection;

/**
 * Spring Boot {@link ConfigurationProperties} for
 * <em>spring.yarn.appmaster</em>.
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties(value = "spring.yarn.appmaster")
public class SpringYarnAppmasterProperties {

	private boolean keepContextAlive = true;
	private int containerCount = 1;
	private String appmasterClass;
	private ContainerClusterProperties containercluster;

	public String getAppmasterClass() {
		return appmasterClass;
	}

	public void setAppmasterClass(String appmasterClass) {
		this.appmasterClass = appmasterClass;
	}

	public boolean isKeepContextAlive() {
		return keepContextAlive;
	}

	public void setKeepContextAlive(boolean keepContextAlive) {
		this.keepContextAlive = keepContextAlive;
	}

	public int getContainerCount() {
		return containerCount;
	}

	public void setContainerCount(int containerCount) {
		this.containerCount = containerCount;
	}

	public ContainerClusterProperties getContainercluster() {
		return containercluster;
	}

	public void setContainercluster(ContainerClusterProperties containercluster) {
		this.containercluster = containercluster;
	}

	public static class ContainerClusterProperties {

		private boolean enabled;

		private Map<String, ContainerClustersProperties> clusters;

		public Map<String, ContainerClustersProperties> getClusters() {
			return clusters;
		}

		public void setClusters(Map<String, ContainerClustersProperties> clusters) {
			this.clusters = clusters;
		}

		public boolean isEnabled() {
			return enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
		}

	}

	public static class ContainerClustersProperties {

		private SpringYarnAppmasterResourceProperties resource;

		private SpringYarnAppmasterLaunchContextProperties launchcontext;

		private SpringYarnAppmasterLocalizerProperties localizer;

		private ContainerClustersProjectionProperties projection;

		public SpringYarnAppmasterResourceProperties getResource() {
			return resource;
		}

		public void setResource(SpringYarnAppmasterResourceProperties resource) {
			this.resource = resource;
		}

		public SpringYarnAppmasterLaunchContextProperties getLaunchcontext() {
			return launchcontext;
		}

		public void setLaunchcontext(SpringYarnAppmasterLaunchContextProperties launchcontext) {
			this.launchcontext = launchcontext;
		}

		public SpringYarnAppmasterLocalizerProperties getLocalizer() {
			return localizer;
		}

		public void setLocalizer(SpringYarnAppmasterLocalizerProperties localizer) {
			this.localizer = localizer;
		}

		public ContainerClustersProjectionProperties getProjection() {
			return projection;
		}

		public void setProjection(ContainerClustersProjectionProperties projection) {
			this.projection = projection;
		}

	}

	public static class ContainerClustersProjectionProperties {

		private String type = DefaultGridProjection.REGISTERED_NAME;

		private ContainerClustersProjectionDataProperties data;

		public void setType(String type) {
			this.type = type;
		}

		public String getType() {
			return type;
		}

		public void setData(ContainerClustersProjectionDataProperties data) {
			this.data = data;
		}

		public ContainerClustersProjectionDataProperties getData() {
			return data;
		}

	}

	public static class ContainerClustersProjectionDataProperties {

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

}
