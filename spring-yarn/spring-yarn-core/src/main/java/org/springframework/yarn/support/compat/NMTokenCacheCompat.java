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
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.springframework.util.ReflectionUtils;

/**
 * Compat class for {@link NMTokenCache}.
 *
 * @author Janne Valkealahti
 *
 */
public class NMTokenCacheCompat {

	private final static Log log = LogFactory.getLog(NMTokenCacheCompat.class);

	private static NMTokenCache nmTokenCache;

	/** NMTokenCache init lock */
	private final static Object lock = new Object();

	/**
	 * Gets the Hadoop {@code NMTokenCache}. Access token cache via singleton
	 * method if exists, otherwise assumes that class is hadoop vanilla
	 * implementation with static methods where we can just use default
	 * constructor and further access for those static method would
	 * got via instance itself.
	 * <p>
	 * This is due to a fact that i.e. cloudera modified this class by
	 * removing static methods and made access to it via singleton pattern.
	 *
	 * @return the token cache
	 */
	public static NMTokenCache getNMTokenCache() {
		if (nmTokenCache == null) {
			synchronized (lock) {
				if (nmTokenCache == null) {
					Method singletonMethod = ReflectionUtils.findMethod(NMTokenCache.class, "getSingleton");
					if (singletonMethod != null) {
						if (log.isDebugEnabled()) {
							log.debug("Creating NMTokenCache using NMTokenCache.getSingleton()");
						}
						nmTokenCache = (NMTokenCache) ReflectionUtils.invokeMethod(singletonMethod, null);
					} else {
						log.debug("Creating NMTokenCache using constructor, further access via instance static methods.");
						nmTokenCache = new NMTokenCache();
					}
				}
			}
		}
		return nmTokenCache;
	}

}
