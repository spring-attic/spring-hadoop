/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.springframework.yarn.boot.actuate.endpoint;

import java.util.Collections;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.yarn.event.ContainerRegisterEvent;
import org.springframework.yarn.event.YarnEventPublisher;

/**
 * {@link Endpoint} handling graceful shutdown of YARN application.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnContainerRegisterEndpoint {

  public static final String ENDPOINT_ID = "yarn_containerregister";

  private static final Log log = LogFactory.getLog(YarnContainerRegisterEndpoint.class);

  private YarnEventPublisher yarnEventPublisher;

  public Map<String, Object> invoke() {
    return Collections.<String, Object>singletonMap("message", "Use POST to report yourself");
  }

  /**
   * Do a registration of a container id and its tracking url. This will initiate an
   * {@link ContainerRegisterEvent} via {@link YarnEventPublisher} if enabled.
   *
   * @param containerId the container id
   * @param trackUrl the track url
   */
  public void register(String containerId, String trackUrl) {
    if (yarnEventPublisher != null) {
      yarnEventPublisher.publishEvent(new ContainerRegisterEvent(this, containerId, trackUrl));
    } else {
      log.warn("Container Id registration not done as no available yarnEventPublisher");
    }
  }

  /**
   * Sets the yarn event publisher.
   *
   * @param yarnEventPublisher the new yarn event publisher
   */
  @Autowired(required=false)
  public void setYarnEventPublisher(YarnEventPublisher yarnEventPublisher) {
    log.debug("Setting yarnEventPublisher=[" + yarnEventPublisher + "]");
    this.yarnEventPublisher = yarnEventPublisher;
  }

}
