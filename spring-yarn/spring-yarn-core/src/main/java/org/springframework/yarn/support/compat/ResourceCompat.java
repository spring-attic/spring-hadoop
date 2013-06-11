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
package org.springframework.yarn.support.compat;

import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.springframework.util.ReflectionUtils;

/**
 * Compat class for {@link Resource}.
 *
 * @author Janne Valkealahti
 *
 */
public class ResourceCompat {

	private final static Log log = LogFactory.getLog(ResourceCompat.class);

	/** Method for {@link Resource#setVirtualCores(int)}*/
	private static Method setVirtualCores;

	/**
	 * Invokes {@link Resource#setVirtualCores(int)}.
	 *
	 * @param resource the target object
	 * @param vCores the vCores
	 */
	public static void setVirtualCores(Resource resource, int vCores) {
		if (setVirtualCores == null) {
			setVirtualCores = ReflectionUtils.findMethod(Resource.class, "setVirtualCores", int.class);
		}
		if (setVirtualCores != null) {
			ReflectionUtils.invokeMethod(setVirtualCores, resource, vCores);
		} else {
			log.warn("Method setVirtualCores(int) is not implemented");
		}
	}

}
