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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.springframework.core.OrderComparator;
import org.springframework.yarn.YarnSystemException;
import org.springframework.yarn.am.AppmasterCmOperations;
import org.springframework.yarn.am.AppmasterCmTemplate;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.fs.ResourceLocalizer;
import org.springframework.yarn.support.LifecycleObjectSupport;

/**
 * The base class for Container launcher implementations.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractLauncher extends LifecycleObjectSupport {

	/** Yarn configuration */
	private Configuration configuration;

	/** Environment variables for the process */
	private Map<String, String> environment;

	/** Resource localizer for the containers */
	private ResourceLocalizer resourceLocalizer;

	/** Interceptors when communicating with service */
	private final ContainerLauncherInterceptorList interceptors =
			new ContainerLauncherInterceptorList();

	@Override
	protected void onInit() throws Exception {
		super.onInit();

	}

	/**
	 * Gets the Yarn configuration.
	 *
	 * @return the Yarn configuration
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Sets the Yarn configuration.
	 *
	 * @param configuration the new Yarn configuration
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Gets the environment.
	 *
	 * @return the environment
	 */
	public Map<String, String> getEnvironment() {
		return environment;
	}

	/**
	 * Sets the environment.
	 *
	 * @param environment the environment
	 */
	public void setEnvironment(Map<String, String> environment) {
		this.environment = environment;
	}

	/**
	 * Sets the resource localizer.
	 *
	 * @param resourceLocalizer the new resource localizer
	 */
	public void setResourceLocalizer(ResourceLocalizer resourceLocalizer) {
		this.resourceLocalizer = resourceLocalizer;
	}

	/**
	 * Gets the resource localizer.
	 *
	 * @return the resource localizer
	 */
	public ResourceLocalizer getResourceLocalizer() {
		return resourceLocalizer;
	}

	/**
	 * Set the list of channel interceptors. This will clear any
	 * existing interceptors.
	 *
	 * @param interceptors the new interceptors
	 */
	public void setInterceptors(List<ContainerLauncherInterceptor> interceptors) {
		Collections.sort(interceptors, new OrderComparator());
		this.interceptors.set(interceptors);
	}

	/**
	 * Add a service interceptor to the end of the list.
	 *
	 * @param interceptor the interceptor
	 */
	public void addInterceptor(ContainerLauncherInterceptor interceptor) {
		this.interceptors.add(interceptor);
	}

	/**
	 * Constructs a new {@link AppmasterCmOperations} template.
	 *
	 * @param container the {@link Container}
	 * @return the constructed {@link AppmasterCmOperations} template
	 */
	protected AppmasterCmOperations getCmTemplate(Container container) {
		try {
			AppmasterCmTemplate template = new AppmasterCmTemplate(getConfiguration(), container);
			template.afterPropertiesSet();
			return template;
		} catch (Exception e) {
			throw new YarnSystemException("Unable to create AppmasterCmTemplate", e);
		}
	}

	/**
	 * Exposes the interceptor list for subclasses.
	 *
	 * @return the interceptors
	 */
	protected ContainerLauncherInterceptorList getInterceptors() {
		return this.interceptors;
	}

	/**
	 * Convenient wrapper for interceptor list.
	 */
	protected class ContainerLauncherInterceptorList {

		/** Actual list of interceptors */
		private final List<ContainerLauncherInterceptor> interceptors =
				new CopyOnWriteArrayList<ContainerLauncherInterceptor>();

		/**
		 * Sets the interceptors, clears any existing interceptors.
		 *
		 * @param interceptors the list of interceptors
		 * @return <tt>true</tt> if interceptor list changed as a result of the call
		 */
		public boolean set(List<ContainerLauncherInterceptor> interceptors) {
			synchronized (interceptors) {
				interceptors.clear();
				return interceptors.addAll(interceptors);
			}
		}

		/**
		 * Adds interceptor to the list.
		 *
		 * @param interceptor the interceptor
		 * @return <tt>true</tt> (as specified by {@link Collection#add})
		 */
		public boolean add(ContainerLauncherInterceptor interceptor) {
			return interceptors.add(interceptor);
		}

		/**
		 * Handles the pre launch calls.
		 *
		 * @param context the container launch context
		 * @param container the container
		 * @return the final modified context or <code>null</code> if interceptor broke the chain
		 */
		public ContainerLaunchContext preLaunch(Container container, ContainerLaunchContext context) {
			for (ContainerLauncherInterceptor interceptor : interceptors) {
				context = interceptor.preLaunch(container, context);
				if(context == null) {
					return null;
				}
			}
			return context;
		}

	}

}
