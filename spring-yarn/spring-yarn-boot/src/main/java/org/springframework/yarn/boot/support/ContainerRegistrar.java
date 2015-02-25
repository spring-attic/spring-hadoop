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
package org.springframework.yarn.boot.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.context.embedded.EmbeddedServletContainerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.boot.actuate.endpoint.mvc.domain.ContainerRegisterResource;
import org.springframework.yarn.support.LifecycleObjectSupport;
import org.springframework.yarn.support.NetworkUtils;

/**
 * Component which registers itself with container registrar boot endpoint.
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerRegistrar extends LifecycleObjectSupport implements
		ApplicationListener<EmbeddedServletContainerInitializedEvent> {

	private static final Log log = LogFactory.getLog(ContainerRegistrar.class);

	private String trackUrl;

	private String containerId;

	/**
	 * Instantiates a new container registrar.
	 *
	 * @param trackUrl the track url
	 * @param containerId the container id
	 */
	public ContainerRegistrar(String trackUrl, String containerId) {
		this.trackUrl = trackUrl;
		this.containerId = containerId;
	}

	@Override
	public void onApplicationEvent(EmbeddedServletContainerInitializedEvent event) {
		String namespace = event.getApplicationContext().getNamespace();
		if ("management".equals(namespace)) {
			return;
		}

		int port = event.getEmbeddedServletContainer().getPort();
		try {
			// TODO: need to handle proper network address
			RestTemplate restTemplate = getBeanFactory().getBean(YarnSystemConstants.DEFAULT_ID_RESTTEMPLATE, RestTemplate.class);
			String url = "http://" + NetworkUtils.getDefaultAddress() + ":" + port;
			ContainerRegisterResource request = new ContainerRegisterResource(containerId, url);
			log.info("Registering containerId=[" + containerId + "] with url=[" + url + "]");
			restTemplate.postForObject(trackUrl + "/yarn_containerregister", request, Object.class);
		} catch (Exception e) {
			log.warn("Error registering with appmaster", e);
		}

	}

}
