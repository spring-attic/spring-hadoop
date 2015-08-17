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
package org.springframework.data.hadoop.boot.properties;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.data.hadoop.HadoopSystemConstants;
import org.springframework.data.hadoop.security.SecurityAuthMethod;
import org.springframework.util.StringUtils;

/**
 * Spring Boot {@link ConfigurationProperties} for <em>spring.hadoop</em>.
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties(value = "spring.hadoop")
public class SpringHadoopProperties implements EnvironmentAware {

	private final static Log log = LogFactory.getLog(SpringHadoopProperties.class);

	// javadocs for fields are for boot configuration metadata processor
	// so keep it simple and end with '.'.

	/** Hadoop filesystem uri. */
	private String fsUri;

	/** YARN resource manager host. */
	private String resourceManagerHost;

	/** YARN resource manager scheduler host. */
	private String resourceManagerSchedulerHost;

	/** YARN resource manager port. */
	private Integer resourceManagerPort = 8032;

	/** YARN resource manager scheduler port. */
	private Integer resourceManagerSchedulerPort = 8030;

	/** MapReduce job history address */
	private String jobHistoryAddress;

	/** Additional Spring properties resources to import. */
	private List<String> resources;

	/** Hadoop security and kerberos configuration. */
	private SpringHadoopSecurityProperties security;

	/** Additional Hadoop configuration keys and values. */
	private Map<String, String> config;

	private String syepFsUri;

	private String syepRm;

	private String syepScheduler;

	@Override
	public void setEnvironment(Environment environment) {
		syepFsUri = environment.getProperty(HadoopSystemConstants.FS_ADDRESS);
		syepRm = environment.getProperty(HadoopSystemConstants.RM_ADDRESS);
		syepScheduler = environment.getProperty(HadoopSystemConstants.SCHEDULER_ADDRESS);
	}

	public String getFsUri() {
		if (log.isDebugEnabled()) {
			log.debug("syp fsUri=[" + fsUri + "]");
			log.debug("syep fsUri=[" + syepFsUri + "]");
		}
		return syepFsUri != null ? syepFsUri : fsUri;
	}

	public void setFsUri(String fsUri) {
		this.fsUri = fsUri;
	}

	public String getResourceManagerAddress() {
		return syepRm != null ? syepRm : resourceManagerHost + ":" + getResourceManagerPort();
	}

	public void setResourceManagerAddress(String resourceManagerAddress) {
		String[] split = StringUtils.split(resourceManagerAddress, ":");
		if (split != null && split.length == 2) {
			try {
				resourceManagerPort = Integer.parseInt(split[1]);
				resourceManagerHost = split[0];
			} catch (Exception e) {
			}
		}
	}

	public String getResourceManagerSchedulerAddress() {
		return syepScheduler != null ? syepScheduler : resourceManagerSchedulerHost + ":" + getResourceManagerSchedulerPort();
	}

	public void setResourceManagerSchedulerAddress(String resourceManagerSchedulerAddress) {
		String[] split = StringUtils.split(resourceManagerSchedulerAddress, ":");
		if (split != null && split.length == 2) {
			try {
				resourceManagerSchedulerPort = Integer.parseInt(split[1]);
				resourceManagerSchedulerHost = split[0];
			} catch (Exception e) {
			}
		}
	}

	public String getResourceManagerHost() {
		return resourceManagerHost;
	}

	public void setResourceManagerHost(String resourceManagerHost) {
		this.resourceManagerHost = resourceManagerHost;
		this.resourceManagerSchedulerHost = resourceManagerHost;
	}

	public Integer getResourceManagerPort() {
		return resourceManagerPort;
	}

	public void setResourceManagerPort(Integer resourceManagerPort) {
		this.resourceManagerPort = resourceManagerPort;
	}

	public Integer getResourceManagerSchedulerPort() {
		return resourceManagerSchedulerPort;
	}

	public void setResourceManagerSchedulerPort(Integer resourceManagerSchedulerPort) {
		this.resourceManagerSchedulerPort = resourceManagerSchedulerPort;
	}

	public String getJobHistoryAddress() {
		return jobHistoryAddress;
	}

	public void setJobHistoryAddress(String jobHistoryAddress) {
		this.jobHistoryAddress = jobHistoryAddress;
	}

	public List<String> getResources() {
		return resources;
	}

	public void setResources(List<String> resources) {
		this.resources = resources;
	}

	public SpringHadoopSecurityProperties getSecurity() {
		return security;
	}

	public void setSecurity(SpringHadoopSecurityProperties security) {
		this.security = security;
	}

	public Map<String, String> getConfig() {
		return config;
	}

	public void setConfig(Map<String, String> config) {
		this.config = config;
	}

	public static class SpringHadoopSecurityProperties {

		/** Hadoop security method. */
		private SecurityAuthMethod authMethod;

		/** Kerberos user principal. */
		private String userPrincipal;

		/** Path to kerberos user keytab file. */
		private String userKeytab;

		/** Hadoop namenode kerberos principal. */
		private String namenodePrincipal;

		/** Hadoop resource manager kerberos principal. */
		private String rmManagerPrincipal;

		public SecurityAuthMethod getAuthMethod() {
			return authMethod;
		}

		public void setAuthMethod(String authMethod) {
			if (StringUtils.hasText(authMethod)) {
				this.authMethod = SecurityAuthMethod.valueOf(authMethod.toUpperCase());
			}
		}

		public String getUserPrincipal() {
			return userPrincipal;
		}

		public void setUserPrincipal(String userPrincipal) {
			this.userPrincipal = userPrincipal;
		}

		public String getUserKeytab() {
			return userKeytab;
		}

		public void setUserKeytab(String userKeytab) {
			this.userKeytab = userKeytab;
		}

		public String getNamenodePrincipal() {
			return namenodePrincipal;
		}

		public void setNamenodePrincipal(String namenodePrincipal) {
			this.namenodePrincipal = namenodePrincipal;
		}

		public String getRmManagerPrincipal() {
			return rmManagerPrincipal;
		}

		public void setRmManagerPrincipal(String rmManagerPrincipal) {
			this.rmManagerPrincipal = rmManagerPrincipal;
		}

	}

}
