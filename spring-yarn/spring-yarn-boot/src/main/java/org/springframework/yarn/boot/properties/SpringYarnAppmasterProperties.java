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

		private Map<String, ContainerClustersProperties> clusters;

		public Map<String, ContainerClustersProperties> getClusters() {
			return clusters;
		}

		public void setClusters(Map<String, ContainerClustersProperties> clusters) {
			this.clusters = clusters;
		}

	}

	public static class ContainerClustersProperties {

		private SpringYarnAppmasterResourceProperties resource;

		private SpringYarnAppmasterLaunchContextProperties launchcontext;

		private SpringYarnAppmasterLocalizerProperties localizer;

		private String projectionType;

		private Integer projectionAny;

		private Map<String, Integer> projectionHosts;

		private Map<String, Integer> projectionRacks;

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

		public String getProjectionType() {
			return projectionType;
		}

		public void setProjectionType(String projectionType) {
			this.projectionType = projectionType;
		}

		public Integer getProjectionAny() {
			return projectionAny;
		}

		public void setProjectionAny(Integer projectionAny) {
			this.projectionAny = projectionAny;
		}

		public Map<String, Integer> getProjectionHosts() {
			return projectionHosts;
		}

		public void setProjectionHosts(Map<String, Integer> projectionHosts) {
			this.projectionHosts = projectionHosts;
		}

		public Map<String, Integer> getProjectionRacks() {
			return projectionRacks;
		}

		public void setProjectionRacks(Map<String, Integer> projectionRacks) {
			this.projectionRacks = projectionRacks;
		}

	}

}
