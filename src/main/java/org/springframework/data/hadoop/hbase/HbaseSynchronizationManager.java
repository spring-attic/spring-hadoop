/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.hadoop.hbase;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.springframework.aop.scope.ScopedObject;
import org.springframework.core.InfrastructureProxy;
import org.springframework.core.NamedThreadLocal;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * @author Costin Leau
 */
public abstract class HbaseSynchronizationManager {

	private static final Log logger = LogFactory.getLog(HbaseSynchronizationManager.class);

	private static final ThreadLocal<Map<String, HTableInterface>> resources = new NamedThreadLocal<Map<String, HTableInterface>>(
			"Bound resources");

	private static final boolean aopAvailable = ClassUtils.isPresent("org.springframework.aop.scope.ScopedObject",
			HbaseSynchronizationManager.class.getClassLoader());


	public static boolean hasResource(Object key) {
		// TransactionSynchronizationUtils.unwrapResourceIfNecessary(key);
		Object actualKey = unwrapResourceIfNecessary(key);
		Object value = doGetResource(actualKey);
		return (value != null);
	}


	public static HTableInterface getResource(Object key) {
		return doGetResource(unwrapResourceIfNecessary(key));
	}

	/**
	 * Actually check the value of the resource that is bound for the given key.
	 */
	private static HTableInterface doGetResource(Object actualKey) {
		Map<String, HTableInterface> tables = resources.get();
		if (tables == null) {
			return null;
		}
		return tables.get(actualKey);
	}

	/**
	 * Bind the given resource for the given key to the current thread.
	 * @param key the key to bind the value to (usually the resource factory)
	 * @param value the value to bind (usually the active resource object)
	 * @throws IllegalStateException if there is already a value bound to the thread
	 * @see ResourceTransactionManager#getResourceFactory()
	 */
	public static void bindResource(String key, HTableInterface value) throws IllegalStateException {
		String actualKey = unwrapResourceIfNecessary(key);
		Assert.notNull(value, "Value must not be null");
		Map<String, HTableInterface> map = resources.get();
		// set ThreadLocal Map if none found
		if (map == null) {
			map = new LinkedHashMap<String, HTableInterface>();
			resources.set(map);
		}
		HTableInterface oldValue = map.put(actualKey, value);
		if (oldValue != null) {
			throw new IllegalStateException("Already value [" + oldValue + "] for key [" +
					actualKey + "] bound to thread [" + Thread.currentThread().getName() + "]");
		}
		if (logger.isTraceEnabled()) {
			logger.trace("Bound value [" + value + "] for key [" + actualKey + "] to thread [" +
					Thread.currentThread().getName() + "]");
		}
	}

	/**
	 * Unbind a resource for the given key from the current thread.
	 * @param key the key to unbind (usually the resource factory)
	 * @return the previously bound value (usually the active resource object)
	 * @throws IllegalStateException if there is no value bound to the thread
	 * @see ResourceTransactionManager#getResourceFactory()
	 */
	public static HTableInterface unbindResource(String key) throws IllegalStateException {
		Object actualKey = unwrapResourceIfNecessary(key);
		HTableInterface value = doUnbindResource(actualKey);
		if (value == null) {
			throw new IllegalStateException(
					"No value for key [" + actualKey + "] bound to thread [" + Thread.currentThread().getName() + "]");
		}
		return value;
	}

	/**
	 * Unbind a resource for the given key from the current thread.
	 * @param key the key to unbind (usually the resource factory)
	 * @return the previously bound value, or <code>null</code> if none bound
	 */
	public static Object unbindResourceIfPossible(Object key) {
		Object actualKey = unwrapResourceIfNecessary(key);
		return doUnbindResource(actualKey);
	}

	/**
	 * Actually remove the value of the resource that is bound for the given key.
	 */
	private static HTableInterface doUnbindResource(Object actualKey) {
		Map<String, HTableInterface> map = resources.get();
		if (map == null) {
			return null;
		}
		HTableInterface value = map.remove(actualKey);
		// Remove entire ThreadLocal if empty...
		if (map.isEmpty()) {
			resources.remove();
		}

		if (value != null && logger.isTraceEnabled()) {
			logger.trace("Removed value [" + value + "] for key [" + actualKey + "] from thread [" +
					Thread.currentThread().getName() + "]");
		}
		return value;
	}
	

	public static Set<String> getTableNames() {
		return Collections.unmodifiableSet(resources.get().keySet());
	}

	/**
	 * Unwrap the given resource handle if necessary; otherwise return
	 * the given handle as-is.
	 * @see org.springframework.core.InfrastructureProxy#getWrappedObject()
	 */
	static <T> T unwrapResourceIfNecessary(Object resource) {
		Assert.notNull(resource, "Resource must not be null");
		Object resourceRef = resource;
		// unwrap infrastructure proxy
		if (resourceRef instanceof InfrastructureProxy) {
			resourceRef = ((InfrastructureProxy) resourceRef).getWrappedObject();
		}
		if (aopAvailable) {
			// now unwrap scoped proxy
			resourceRef = ScopedProxyUnwrapper.unwrapIfNecessary(resourceRef);
		}
		return (T) resourceRef;
	}

	/**
	 * Inner class to avoid hard-coded dependency on AOP module.
	 */
	private static class ScopedProxyUnwrapper {

		public static Object unwrapIfNecessary(Object resource) {
			if (resource instanceof ScopedObject) {
				return ((ScopedObject) resource).getTargetObject();
			}
			else {
				return resource;
			}
		}
	}

	public static HTable getResource(String tableName) {
		throw new UnsupportedOperationException();
	}
}