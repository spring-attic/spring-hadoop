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
package org.springframework.yarn.boot.actuate.endpoint.mvc;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.yarn.boot.actuate.endpoint.YarnContainerRegisterEndpoint;
import org.springframework.yarn.boot.actuate.endpoint.mvc.domain.ContainerRegisterResource;

/**
 * A {@link RestController} adding specific rest API used to handle graceful YARN application
 * shutdown.
 *
 * @author Janne Valkealahti
 *
 */
@RestController
@RequestMapping(YarnContainerRegisterEndpoint.ENDPOINT_ID)
public class YarnContainerRegisterMvcEndpoint {

  private final YarnContainerRegisterEndpoint delegate;

  /**
   * Instantiates a new yarn container register mvc endpoint.
   *
   * @param delegate the delegate endpoint
   */
  public YarnContainerRegisterMvcEndpoint(YarnContainerRegisterEndpoint delegate) {
    this.delegate = delegate;
  }

  @GetMapping
  @ResponseBody
  public Object invoke() {
    return delegate.invoke();
  }

  @PostMapping
  public HttpEntity<Void> register(@RequestBody ContainerRegisterResource request) {
    delegate.register(request.getContainerId(), request.getTrackUrl());
    HttpHeaders responseHeaders = new HttpHeaders();
    return new ResponseEntity<>(responseHeaders, HttpStatus.ACCEPTED);
  }

}
