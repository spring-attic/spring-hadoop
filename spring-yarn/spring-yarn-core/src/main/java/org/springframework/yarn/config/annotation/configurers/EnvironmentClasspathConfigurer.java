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
package org.springframework.yarn.config.annotation.configurers;

import java.util.List;

import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurerBuilder;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentConfigurer;

/**
 * {@link AnnotationConfigurerBuilder} for configuring classpath environment variable.
 *
 * <p>
 * Typically configuration is shown below.
 * <p>
 * <pre>
 * &#064;Configuration
 * &#064;EnableYarn(enable=Enable.APPMASTER)
 * static class Config extends SpringYarnConfigurerAdapter {
 *
 *   &#064;Override
 *   public void configure(YarnEnvironmentConfigurer environment) throws Exception {
 *     environment
 *       .withClasspath()
 *         .includeBaseDirectory(true)
 *         .useDefaultYarnClasspath(true)
 *         .defaultYarnAppClasspath("my:cp:entries")
 *         .delimiter(":")
 *         .entries("entry1", "entry2")
 *         .entry("entry3");
 *   }
 *
 * }
 * </pre>
 *
 * @author Janne Valkealahti
 *
 */
public interface EnvironmentClasspathConfigurer extends AnnotationConfigurerBuilder<YarnEnvironmentConfigurer> {

	/**
	 * Specify a new classpath entry.
	 *
	 * @param entry the classpath entry
	 * @return {@link EnvironmentClasspathConfigurer} for chaining
	 */
	EnvironmentClasspathConfigurer entry(String entry);

	/**
	 * Specify a new classpath entries.
	 *
	 * @param entries the classpath entries
	 * @return {@link EnvironmentClasspathConfigurer} for chaining
	 */
	EnvironmentClasspathConfigurer entries(String... entries);

	/**
	 * Specify a new classpath entries.
	 *
	 * @param entries the classpath entries
	 * @return {@link EnvironmentClasspathConfigurer} for chaining
	 */
	EnvironmentClasspathConfigurer entries(List<String> entries);

	/**
	 * Specify if default yarn classpath entries should be added.
	 *
	 * @param useDefaultClasspath the use default classpath
	 * @return {@link EnvironmentClasspathConfigurer} for chaining
	 */
	EnvironmentClasspathConfigurer useDefaultYarnClasspath(boolean useDefaultClasspath);

	/**
	 * Specify a default yarn application classpath
	 *
	 * @param defaultClasspath the default classpath
	 * @return {@link EnvironmentClasspathConfigurer} for chaining
	 */
	EnvironmentClasspathConfigurer defaultYarnAppClasspath(String defaultClasspath);

	/**
	 * Specify if base directory should be added in classpath.
	 *
	 * @param includeBaseDirectory the include base directory
	 * @return {@link EnvironmentClasspathConfigurer} for chaining
	 */
	EnvironmentClasspathConfigurer includeBaseDirectory(boolean includeBaseDirectory);

	/**
	 * Specify a delimiter used in a classpath.
	 *
	 * @param delimiter the delimiter
	 * @return {@link EnvironmentClasspathConfigurer} for chaining
	 */
	EnvironmentClasspathConfigurer delimiter(String delimiter);

}
