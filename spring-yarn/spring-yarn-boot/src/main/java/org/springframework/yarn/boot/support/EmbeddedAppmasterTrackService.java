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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.context.embedded.AnnotationConfigEmbeddedWebApplicationContext;
import org.springframework.boot.context.embedded.EmbeddedServletContainer;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.yarn.am.AppmasterTrackService;
import org.springframework.yarn.support.NetworkUtils;

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

	private EmbeddedServletContainer embeddedServletContainer;

	private long waitTime;

	/**
	 * Instantiates a new embedded appmaster track service with
	 * default wait time of 60 seconds.
	 */
	public EmbeddedAppmasterTrackService() {
		this(DEFAULT_WAIT_TIME);
	}

	/**
	 * Instantiates a new embedded appmaster track service.
	 *
	 * @param waitTime the wait time in millis
	 */
	public EmbeddedAppmasterTrackService(long waitTime) {
		this.waitTime = waitTime;
	}

	@Override
	public String getTrackUrl() {
		if (embeddedServletContainer == null) {
			log.warn("Request for track url but unable to delegate because embeddedServletContainer is not set, returning null.");
			return null;
		}
		long now = System.currentTimeMillis();
		while(now + waitTime > System.currentTimeMillis()) {
			int port = embeddedServletContainer.getPort();
			if (log.isDebugEnabled()) {
				log.debug("Polling port from EmbeddedServletContainer port=" + port);
			}
			if (port > 0) {
				String url = "http://" + NetworkUtils.getDefaultAddress() + ":" + port;
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
		if (source instanceof AnnotationConfigEmbeddedWebApplicationContext) {
			embeddedServletContainer = ((AnnotationConfigEmbeddedWebApplicationContext) source)
					.getEmbeddedServletContainer();
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
