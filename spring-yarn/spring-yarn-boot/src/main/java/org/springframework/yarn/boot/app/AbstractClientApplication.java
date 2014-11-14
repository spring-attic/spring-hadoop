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
package org.springframework.yarn.boot.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.springframework.boot.SpringApplication;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Base class for client applications.
 *
 * @author Janne Valkealahti
 *
 * @param <T> the type of a sub-class
 */
public abstract class AbstractClientApplication<R, T extends AbstractClientApplication<R, T>> implements ClientApplicationRunner<R> {

	protected String applicationVersion;
	protected String applicationBaseDir;
	protected List<Object> sources = new ArrayList<Object>();
	protected List<String> profiles = new ArrayList<String>();
	protected Properties appProperties = new Properties();

	@Override
	public R run() {
		return run(new String[0]);
	}

	@Override
	public abstract R run(String... args);

	/**
	 * Sets an application version to be used by a builder.
	 *
	 * @param applicationVersion the application version
	 * @return the T for chaining
	 */
	public T applicationVersion(String applicationVersion) {
		Assert.state(StringUtils.hasText(applicationVersion), "Application version must not be empty");
		this.applicationVersion = applicationVersion;
		return getThis();
	}

	/**
	 * Sets an Applications base directory to be used by a builder.
	 *
	 * @param applicationBaseDir the applications base directory
	 * @return the T for chaining
	 */
	public T applicationBaseDir(String applicationBaseDir) {
		// can be empty because value may come from an existing properties
		this.applicationBaseDir = applicationBaseDir;
		return getThis();
	}

	/**
	 * Sets an additional sources to by used when running
	 * an {@link SpringApplication}.
	 *
	 * @param sources the additional sources for Spring Application
	 * @return the T for chaining
	 */
	public T sources(Object... sources) {
		if (!ObjectUtils.isEmpty(sources)) {
			this.sources.addAll(Arrays.asList(sources));
		}
		return getThis();
	}

	/**
	 * Sets an additional profiles to be used when running
	 * an {@link SpringApplication}.
	 *
	 * @param profiles the additional profiles for Spring Application
	 * @return the T for chaining
	 */
	public T profiles(String ... profiles) {
		if (!ObjectUtils.isEmpty(profiles)) {
			this.profiles.addAll(Arrays.asList(profiles));
		}
		return getThis();
	}

	/**
	 * Sets application properties which will be passed into a Spring Boot
	 * environment. Properties are placed with a priority which is just below
	 * command line arguments put above all other properties.
	 * <p>
	 * Effectively this means that these properties allow to override all
	 * existing properties but still doesn't override properties based on
	 * command-line arguments. Command-line arguments in this context are the
	 * ones passed to <code>run</code> method in a sub-class.
	 *
	 * @param appProperties the app properties
	 * @return the T for chaining
	 */
	public T appProperties(Properties appProperties) {
		this.appProperties = appProperties;
		return getThis();
	}

	/**
	 * Gets the instance of this defined by a sub-class. Needed for methods in
	 * this abstract class to be able to return correct type for method
	 * chaining.
	 *
	 * @return the this instance
	 */
	protected abstract T getThis();

}
