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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.context.ApplicationContext;
import org.springframework.util.Assert;

/**
 * Static wrapper for a {@link ContextLoader} to enable Spring components to
 * share an application context in a Hadoop environment.
 * 
 * @author Dave Syer
 * 
 */
public class HadoopApplicationContextUtils {

	private static ContextLoader loader = new DefaultContextLoader();

	/**
	 * Get a bean instance by type and property name from an existing
	 * {@link ApplicationContext}. The context must have been created previously
	 * with {@link #getBean(Configuration, Class)} or {@link #getJob(Class)} (or
	 * one of their overloaded variants). There is no obligation to release a
	 * bean obtained in this way.
	 * 
	 * @param <T> the expected type of the result
	 * @param configuration the current configuration which has a Spring config
	 * location
	 * @param type the expected type of the result
	 * @param property the property name
	 * 
	 * @return the bean from an existing application context
	 * 
	 * @throws IllegalStateException if the context does not already exist, or
	 * if the bean cannot be found
	 */
	public static <T> T getExistingBean(Configuration configuration, Class<T> type, String property) {
		T bean = loader.getBean(configuration, type, false, property);
		assertNotNull(bean, type, property);
		return bean;
	}

	/**
	 * Get a bean instance by type from an existing {@link ApplicationContext}.
	 * The context must have been created previously with
	 * {@link #getBean(Configuration, Class)} or {@link #getJob(Class)} (or one
	 * of their overloaded variants). There is no obligation to release a bean
	 * obtained in this way.
	 * 
	 * @param <T> the expected type of the result
	 * @param configuration the current configuration which has a Spring config
	 * location
	 * @param type the expected type of the result
	 * 
	 * @return the bean from an existing application context
	 * 
	 * @throws IllegalStateException if the context does not already exist, or
	 * if the bean cannot be found
	 */
	public static <T> T getExistingBean(Configuration configuration, Class<T> type) {
		return getExistingBean(configuration, type, null);
	}

	/**
	 * <p>
	 * Get a bean instance by type and property name from an existing
	 * {@link ApplicationContext} or create the context if it does not exist.
	 * The bean is searched for in the following locations, until one is found:
	 * <ul>
	 * <li>As a property of a factory bean with id=<code>jobName</code>. The
	 * property name is as supplied.</li>
	 * <li>As a top-level bean with id equal to the property name supplied.</li>
	 * <li>As a top-level bean with id equal to the job name prepended to the
	 * property name supplied.</li>
	 * <li>As a top level bean with id constructed from the previous two steps
	 * using the type name instead of the property name supplied</li>
	 * </ul>
	 * </p>
	 * <p>
	 * For example, a bean of type Mapper for a job named <code>myJob</code> can
	 * be located as &amp;myJob.mapper
	 * </p>
	 * <p>
	 * The context is created from a Spring config location determined from the
	 * configuration supplied (class, package or xml resource). There is an
	 * obligation to release a bean obtained in this way by using
	 * {@link #releaseContext(Configuration)} (or one of its variants), e.g.
	 * in a finally block or clean up method.
	 * </p>
	 * 
	 * @param <T> the expected type of the result
	 * @param configuration the current configuration which has a Spring config
	 * location
	 * @param type the expected type of the result
	 * @param property the property name
	 * 
	 * @return the bean from an existing application context
	 * 
	 * @throws IllegalStateException if the context does not already exist, or
	 * if the bean cannot be found
	 */
	public static <T> T getBean(Configuration configuration, Class<T> type, String property) {
		T bean = loader.getBean(configuration, type, true, property);
		assertNotNull(bean, type, property);
		return bean;
	}

	/**
	 * <p>
	 * Get a bean instance by type from an existing {@link ApplicationContext}
	 * or create the context if it does not exist. The bean is searched foras
	 * per {@link #getBean(Configuration, Class, String)} using a property name
	 * equal to the type name (uncapitalized).
	 * </p>
	 * <p>
	 * For example, a bean of type <code>Mapper</code> for a job named
	 * <code>myJob</code>(created via a factory bean <code>&amp;myJob</code>)
	 * can be located as <code>&amp;myJob.mapper</code>, or <code>mapper</code>,
	 * or <code>myJobMapper</code>.
	 * </p>
	 * The context is created from a Spring config location determined from the
	 * configuration supplied (class, package or xml resource). There is an
	 * obligation to release a bean obtained in this way by using
	 * {@link #releaseContext(Configuration)} (or one of its variants), e.g.
	 * in a finally block or clean up method. </p>
	 * 
	 * @param <T> the expected type of the result
	 * @param configuration the current configuration which has a Spring config
	 * location
	 * @param type the expected type of the result
	 * 
	 * @return the bean from an existing application context
	 * 
	 * @throws IllegalStateException if the context does not already exist, or
	 * if the bean cannot be found
	 */
	public static <T> T getBean(Configuration configuration, Class<T> type) {
		return getBean(configuration, type, null);
	}

	public static void releaseContext(Configuration configuration) {
		loader.releaseContext(configuration);
	}

	public static Job getJob(Object configLocation) {
		return getJob(configLocation, null, null);
	}

	public static Job getJob(Object configLocation, String jobName) {
		return getJob(configLocation, null, jobName);
	}

	public static Job getJob(Object configLocation, Configuration configuration) {
		return getJob(configLocation, configuration, null);
	}

	public static Job getJob(Object configLocation, Configuration configuration, String jobName) {
		if (configuration==null) {
			configuration = new Configuration();
		}
		Job job = loader.getJob(configLocation, configuration, jobName);
		assertNotNull(job, Job.class, jobName);
		return job;
	}

	public static void releaseJob(Job job) {
		loader.releaseJob(job);
	}

	private static <T> void assertNotNull(T bean, Class<T> type, String property) {
		Assert.state(bean != null, "Required bean of type " + type + " not found"
				+ (property != null ? " with property name=" + property : ""));
	}

}
