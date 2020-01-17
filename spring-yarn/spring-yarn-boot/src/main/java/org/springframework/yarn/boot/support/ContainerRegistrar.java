/*
 * Copyright 2014-2016 the original author or authors.
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
package org.springframework.yarn.boot.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.web.servlet.context.ServletWebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.data.hadoop.util.net.HostInfoDiscovery;
import org.springframework.data.hadoop.util.net.HostInfoDiscovery.HostInfo;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.boot.actuate.endpoint.mvc.domain.ContainerRegisterResource;
import org.springframework.yarn.support.LifecycleObjectSupport;

/**
 * Component which registers itself with container registrar boot endpoint.
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerRegistrar extends LifecycleObjectSupport implements
		ApplicationListener<ServletWebServerInitializedEvent> {

	private static final Log log = LogFactory.getLog(ContainerRegistrar.class);

	private final String trackUrl;
	private final String containerId;
	private final HostInfoDiscovery hostInfoDiscovery;

	/**
	 * Instantiates a new container registrar.
	 *
	 * @param trackUrl the track url
	 * @param containerId the container id
	 * @param hostInfoDiscovery host info discovery
	 */
	public ContainerRegistrar(String trackUrl, String containerId, HostInfoDiscovery hostInfoDiscovery) {
		this.trackUrl = trackUrl;
		this.containerId = containerId;
		this.hostInfoDiscovery = hostInfoDiscovery;
	}

	@Override
	public void onApplicationEvent(ServletWebServerInitializedEvent event) {
		String namespace = event.getApplicationContext().getNamespace();
		if ("management".equals(namespace)) {
			return;
		}

		int port = event.getWebServer().getPort();
		try {
			RestTemplate restTemplate = getBeanFactory().getBean(YarnSystemConstants.DEFAULT_ID_RESTTEMPLATE, RestTemplate.class);
			HostInfo hostInfo = hostInfoDiscovery.getHostInfo();
			String url = "http://" + (hostInfo != null ? hostInfo.getAddress() : "127.0.0.1") + ":" + port;
			ContainerRegisterResource request = new ContainerRegisterResource(containerId, url);
			log.info("Registering containerId=[" + containerId + "] with url=[" + url + "]");
			restTemplate.postForObject(trackUrl + "/yarn_containerregister", request, Object.class);
		} catch (Exception e) {
			log.warn("Error registering with appmaster", e);
		}

	}

}
