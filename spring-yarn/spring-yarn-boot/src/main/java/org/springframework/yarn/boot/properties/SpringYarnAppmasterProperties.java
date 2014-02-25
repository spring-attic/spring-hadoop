package org.springframework.yarn.boot.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot {@link ConfigurationProperties} for
 * <em>spring.yarn.appmaster</em>.
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties(name = "spring.yarn.appmaster")
public class SpringYarnAppmasterProperties {

	private boolean keepContextAlive = true;
	private int containerCount = 1;
	private String appmasterClass;

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

}
