/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.yarn.am.container;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ResourceRequest;

/**
 * Interface for implementations which knows how to resolve
 * container locations. This is meant for similar concepts found
 * from a map/reduce framework to execute containers as close for the
 * input as possible. For example if task main purpose is to access
 * HDFS resources it will be much faster if those containers can be
 * executed on a same host where HDFS file/block is physically placed.
 *
 * @author Janne Valkealahti
 *
 */
public interface ContainerResolver {

	/**
	 * Resolve resource requests.
	 *
	 * @param parameters the parameters
	 * @return the list of {@link ResourceRequest}s
	 */
	List<ResourceRequest> resolve(Map<String, Object> parameters);

}
