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
//	private Map<String, ContainerClusterProperties> containercluster;

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

//	public Map<String, ContainerClusterProperties> getContainercluster() {
//		return containercluster;
//	}
//
//	public void setContainercluster(Map<String, ContainerClusterProperties> containercluster) {
//		this.containercluster = containercluster;
//	}

	public static class ContainerClusterProperties {

		private boolean enabled;

		private Map<String, ContainerClustersProperties> clusters;

		public boolean isEnabled() {
			return enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
		}

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
