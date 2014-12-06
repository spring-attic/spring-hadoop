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
package org.springframework.yarn.boot.support;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.am.container.ContainerRegisterInfo;
import org.springframework.yarn.am.container.ContainerShutdown;

/**
 * {@link ContainerShutdown} implementation which uses boot shutdown
 * rest endpoint to notify context close.
 *
 * @author Janne Valkealahti
 *
 */
public class EndpointContainerShutdown implements ContainerShutdown {

	private static final Log log = LogFactory.getLog(EndpointContainerShutdown.class);

	@Override
	public boolean shutdown(Map<Container, ContainerRegisterInfo> containers) {
		log.info("Shutting down containers using boot shutdown endpoint");
		RestTemplate restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());
		boolean ok = true;
		for (Entry<Container, ContainerRegisterInfo> entry : containers.entrySet()) {
			Container c = entry.getKey();
			ContainerRegisterInfo i = entry.getValue();
			String url = i.getTrackUrl() + "/shutdown";
			log.info("Shutting down container=[" + c + "] using url=[" + url + "]");
			try {
				// TODO: need to integrate auth
				restTemplate.postForObject(url, null, Void.class);
			} catch (Exception e) {
				log.warn("Error shutting down container=[" + c + "]");
				ok = false;
			}
		}
		return ok;
	}

}
