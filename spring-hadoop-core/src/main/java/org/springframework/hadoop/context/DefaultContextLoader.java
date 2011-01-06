/*
 * Copyright 2006-2011 the original author or authors.
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
package org.springframework.hadoop.context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.factory.support.AbstractBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * @author Dave Syer
 * 
 */
public class DefaultContextLoader implements ContextLoader {

	/**
	 * Configuration key for Spring config location.
	 */
	public static final String SPRING_CONFIG_LOCATION = "spring.config.location";

	/**
	 * Configuration key for job name (in case distinct from normal job name).
	 */
	public static final String SPRING_JOB_NAME = "spring.job.name";

	private ConcurrentMap<String, ApplicationContextReference> contexts = new ConcurrentHashMap<String, ApplicationContextReference>();

	public <T> T getBean(Configuration configuration, Class<T> type, boolean createContext) {
		return getBean(configuration, type, createContext, null);
	}
	
	public <T> T getBean(Configuration configuration, Class<T> type, boolean createContext, String property) {
		ApplicationContextReference reference = findApplicationContext(configuration, createContext);
		T bean = getBean(reference.getContext(), configuration, type, property);
		if (bean != null && createContext) {
			reference.increment();
		}
		return bean;
	}

	public void releaseContext(Configuration configuration) {
		ApplicationContextReference reference = getApplicationContext(configuration);
		if (reference == null) {
			return;
		}
		if (reference.decrement() == 0) {
			remove(configuration);
		}
	}

	public Job getJob(Class<?> configLocation, String jobName) {
		Assert.notNull(configLocation, "A config location must be provided");
		ApplicationContextReference reference = findApplicationContext(configLocation.getName(), true);
		return getJobInternal(reference, jobName);
	}

	public Job getJob(Class<?> configLocation) {
		return getJob(configLocation, null);
	}

	public Job getJob(String configLocation, String jobName) {
		Assert.notNull(configLocation, "A config location must be provided");
		ApplicationContextReference reference = findApplicationContext(configLocation, true);
		return getJobInternal(reference, jobName);
	}

	public Job getJob(String configLocation) {
		return getJob(configLocation, null);
	}

	public void releaseJob(Job job) {
		if (job == null) {
			return;
		}
		Configuration configuration = job.getConfiguration();
		releaseContext(configuration);
	}

	private void remove(Configuration configuration) {
		ApplicationContextReference reference = getApplicationContext(configuration);
		if (reference == null) {
			return;
		}
		// TODO: use a cache with expiry instead of always removing immediately
		ApplicationContext context = reference.getContext();
		if (context instanceof ConfigurableApplicationContext) {
			((ConfigurableApplicationContext) context).close();
		}
		contexts.remove(getJobName(configuration));
	}

	private Job getJobInternal(ApplicationContextReference reference, String jobName) {

		Job job = null;
		if (jobName == null) {
			job = reference.getContext().getBean(Job.class);
		}
		else {
			job = reference.getContext().getBean(jobName, Job.class);
		}

		if (job != null) {
			Configuration configuration = job.getConfiguration();
			configuration.set(SPRING_CONFIG_LOCATION, reference.getConfigLocation());
			reference.increment();
			if (job.getJobName() != null) {
				jobName = job.getJobName();
				configuration.set(SPRING_JOB_NAME, jobName);
			}
		}

		return job;

	}

	private <T> T getBeanFromFactoryBean(ApplicationContext context, Configuration configuration, Class<T> type,
			String property) {

		String factory = AbstractBeanFactory.FACTORY_BEAN_PREFIX + getJobName(configuration);

		if (context.containsBean(factory)) {
			BeanWrapperImpl wrapper = new BeanWrapperImpl(context.getBean(factory));
			property = property != null ? property : StringUtils.uncapitalize(type.getSimpleName());
			if (type.isAssignableFrom(wrapper.getPropertyType(property)) && wrapper.isReadableProperty(property)) {
				@SuppressWarnings("unchecked")
				T value = (T) wrapper.getPropertyValue(property);
				return value;
			}
		}

		return null;

	}

	private String getJobName(Configuration configuration) {
		return configuration.get(SPRING_JOB_NAME, "job");
	}

	private String getConfigLocation(Configuration configuration) {
		String jobName = getJobName(configuration);
		String defaultValue = configuration.get(SPRING_CONFIG_LOCATION, "/META-INF/spring/hadoop/" + jobName
				+ "-context.xml");
		return configuration.get(SPRING_CONFIG_LOCATION, defaultValue);
	}

	private ApplicationContextReference findApplicationContext(String path, boolean createContext) {
		if (contexts.containsKey(path)) {
			return contexts.get(path);
		}
		if (!createContext) {
			throw new IllegalStateException("Could not find existing ApplicationContext at [" + path + "]");
		}
		Class<?> configClass = null;
		try {
			configClass = ClassUtils.forName(path, ClassUtils.getDefaultClassLoader());
		}
		catch (ClassNotFoundException e) {
			// ignore
		}
		ApplicationContext context;
		if (configClass != null) {
			context = new AnnotationConfigApplicationContext((Class<?>) configClass);
		}
		else {
			if (path.endsWith(".xml")) {
				context = new ClassPathXmlApplicationContext(path);
			}
			else {
				context = new AnnotationConfigApplicationContext(path);
			}
		}
		contexts.putIfAbsent(path, new ApplicationContextReference(context, path));
		return contexts.get(path);
	}

	private ApplicationContextReference findApplicationContext(Configuration configuration, boolean createContext) {
		return findApplicationContext(getConfigLocation(configuration), createContext);
	}

	private ApplicationContextReference getApplicationContext(Configuration configuration) {
		String jobName = getJobName(configuration);
		return contexts.get(jobName);
	}

	private <T> T getBean(ApplicationContext context, Configuration configuration, Class<T> type, String property) {
		T bean = getBeanFromFactoryBean(context, configuration, type, property);
		if (bean == null) {
			bean = getBeanByName(context, configuration, type, property);
		}
		return bean;
	}

	private <T> T getBeanByName(ApplicationContext context, Configuration configuration, Class<T> type, String property) {
		T bean = null;
		List<String> candidates = Arrays.asList(StringUtils.uncapitalize(type.getSimpleName()),
				StringUtils.uncapitalize(getJobName(configuration)) + type.getSimpleName());
		if (property != null) {
			candidates = new ArrayList<String>(candidates);
			candidates.addAll(
					0,
					Arrays.asList(StringUtils.uncapitalize(property),
							StringUtils.uncapitalize(getJobName(configuration)) + StringUtils.capitalize(property)));
		}

		List<String> names = Arrays.asList(context.getBeanNamesForType(type));
		if (names.size() == 1) {
			bean = context.getBean(type);
		}
		else {
			for (String name : candidates) {
				if (names.contains(name)) {
					bean = context.getBean(name, type);
					break;
				}
			}
		}
		return bean;
	}

	private static class ApplicationContextReference {
		private final ApplicationContext context;

		private final AtomicInteger references = new AtomicInteger();

		private final String configLocation;

		public ApplicationContextReference(ApplicationContext context, Object configLocation) {
			this.context = context;
			this.configLocation = configLocation instanceof Class ? ((Class<?>) configLocation).getName()
					: configLocation.toString();
		}

		public String getConfigLocation() {
			return configLocation;
		}

		public int decrement() {
			int count = references.decrementAndGet();
			return count;
		}

		public int increment() {
			return references.incrementAndGet();
		}

		public ApplicationContext getContext() {
			return context;
		}
	}

}
