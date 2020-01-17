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
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.data.hadoop.util.net.HostInfoDiscovery;
import org.springframework.data.hadoop.util.net.HostInfoDiscovery.HostInfo;
import org.springframework.yarn.am.AppmasterTrackService;

/**
 * A {@link AppmasterTrackService} which delegates to an
 * {@link EmbeddedServletContainer} to find a configured port whether that is
 * hard coded or set to be picked up automatically.
 * <p>
 * {@link EmbeddedServletContainer} is received from an {@link ApplicationEvent}
 * send by boot to notify existence of
 * {@link AnnotationConfigEmbeddedWebApplicationContext}.
 *
 * @author Janne Valkealahti
 *
 */
public class EmbeddedAppmasterTrackService implements AppmasterTrackService, ApplicationListener<ApplicationEvent> {

	private final static Log log = LogFactory.getLog(EmbeddedAppmasterTrackService.class);

	private final static long DEFAULT_WAIT_TIME = 60000;

	private ServletWebServerInitializedEvent embeddedServletContainer;

	private long waitTime;

	private final HostInfoDiscovery hostInfoDiscovery;

	/**
	 * Instantiates a new embedded appmaster track service with
	 * default wait time of 60 seconds.
	 *
	 * @param hostInfoDiscovery host info discovery
	 */
	public EmbeddedAppmasterTrackService(HostInfoDiscovery hostInfoDiscovery) {
		this(DEFAULT_WAIT_TIME, hostInfoDiscovery);
	}

	/**
	 * Instantiates a new embedded appmaster track service.
	 *
	 * @param waitTime the wait time in millis
	 * @param hostInfoDiscovery the host info discovery
	 */
	public EmbeddedAppmasterTrackService(long waitTime, HostInfoDiscovery hostInfoDiscovery) {
		this.waitTime = waitTime;
		this.hostInfoDiscovery = hostInfoDiscovery;
	}

	@Override
	public String getTrackUrl() {
		if (embeddedServletContainer == null) {
			log.warn("Request for track url but unable to delegate because embeddedServletContainer is not set, returning null.");
			return null;
		}
		log.info("Using hostInfoDiscovery " + hostInfoDiscovery);
		long now = System.currentTimeMillis();
		while(now + waitTime > System.currentTimeMillis()) {
			int port = embeddedServletContainer.getWebServer().getPort();
			if (log.isDebugEnabled()) {
				log.debug("Polling port from EmbeddedServletContainer port=" + port);
			}
			if (port > 0) {
				HostInfo hostInfo = hostInfoDiscovery.getHostInfo();
				String url = "http://" + (hostInfo != null ? hostInfo.getAddress() : "127.0.0.1") + ":" + port;
				log.info("Giving out track url as " + url);
				return url;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
		log.warn("Waited " + (System.currentTimeMillis()-now) + " millis for embeddedServletContainer port, returning null.");
		return null;
	}

	@Override
	public void onApplicationEvent(ApplicationEvent event) {
		Object source = event.getSource();
		if (source instanceof ServletWebServerInitializedEvent) {
			embeddedServletContainer = (ServletWebServerInitializedEvent) source;
		}
	}

	/**
	 * Sets the max time waiting for embedded container port.
	 *
	 * @param waitTime the new wait time in millis
	 */
	public void setWaitTime(long waitTime) {
		this.waitTime = waitTime;
	}

}
