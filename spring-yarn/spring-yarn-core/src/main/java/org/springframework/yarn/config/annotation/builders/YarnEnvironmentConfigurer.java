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
package org.springframework.yarn.config.annotation.builders;

import java.io.IOException;

import org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigurer;
import org.springframework.yarn.config.annotation.SpringYarnConfigurerAdapter;
import org.springframework.yarn.config.annotation.configurers.EnvironmentClasspathConfigurer;
import org.springframework.yarn.config.annotation.configurers.DefaultEnvironmentClasspathConfigurer;

/**
 * Interface for {@link YarnEnvironmentBuilder} used from
 * a {@link SpringYarnConfigurerAdapter}.
 * <p>
 * Typically configuration is used as shown below.
 * <p>
 * <pre>
 * &#064;Configuration
 * &#064;EnableYarn
 * static class Config extends SpringYarnConfigurerAdapter {
 *
 *   &#064;Override
 *   public void configure(YarnEnvironmentBuilder environment) throws Exception {
 *     environment
 *       .withClasspath()
 *         .entry("cpEntry1")
 *         .entry("cpEntry2")
 *         .useDefaultYarnClasspath(true);
 *   }
 *
 * }
 * </pre>
 *
 * @author Janne Valkealahti
 *
 */
public interface YarnEnvironmentConfigurer {

	/**
	 * Specify a classpath environment variable.
	 * <p>
	 * Applies a new {@link DefaultEnvironmentClasspathConfigurer} into current
	 * builder. Equivalents between JavaConfig and XML are shown below.
	 *
	 * <p>JavaConfig:
	 * <pre>
	 * public void configure(YarnEnvironmentBuilder environment) throws Exception {
	 *   environment
	 *     .withClasspath()
	 *       .entry("cpEntry1")
	 *       .entry("cpEntry2")
	 *       .useDefaultYarnClasspath(true);
	 * }
	 * </pre>
	 * <p>XML:
	 * <pre>
	 * &lt;yarn:environment>
	 *   &lt;yarn:classpath use-default-yarn-classpath="true" delimiter=":">
	 *     cpEntry1
	 *     cpEntry2
	 *   &lt;/yarn:classpath>
	 * &lt;/yarn:environment>
	 * </pre>
	 *
	 * @return {@link DefaultEnvironmentClasspathConfigurer} for classpath
	 * @throws Exception if error occurred
	 */
	EnvironmentClasspathConfigurer withClasspath() throws Exception;

	/**
	 * Specify an environment variable.
	 *
	 * <p>JavaConfig:
	 * <pre>
	 * public void configure(YarnEnvironmentConfigure environment) throws Exception {
	 *   environment
	 *     .entry("myKey1","myValue1")
	 *     .entry("myKey2","myValue2");
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <pre>
	 * &lt;yarn:environment>
	 *   myKey1=myValue1
	 *   myKey2=myValue2
	 * &lt;/yarn:environment>
	 * </pre>
	 *
	 * @param key The environment key
	 * @param value The environment value
	 * @return {@link YarnEnvironmentConfigurer} for chaining
	 */
	YarnEnvironmentConfigurer entry(String key, String value);

	/**
	 * Specify properties locations.
	 *
	 * <p>JavaConfig:
	 * <pre>
	 * public void configure(YarnEnvironmentConfigure environment) throws Exception {
	 *   environment
	 *     .entry("myKey1","myValue1")
	 *     .entry("myKey2","myValue2")
	 *     .propertiesLocation("cfg-1.properties", "cfg-2.properties");
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <pre>
	 * &lt;yarn:environment properties-location="cfg-1.properties, cfg-2.properties">
	 *   myKey1=myValue1
	 *   myKey2=myValue2
	 * &lt;/yarn:environment>
	 * </pre>
	 *
	 * @param locations The properties file locations
	 * @return {@link YarnEnvironmentConfigurer} for chaining
	 * @throws IOException if error occurred
	 */
	YarnEnvironmentConfigurer propertiesLocation(String... locations) throws IOException;

	/**
	 * Specify if existing system environment variables should
	 * be included automatically.
	 *
	 * <p>JavaConfig:
	 * <pre>
	 * public void configure(YarnEnvironmentConfigure environment) throws Exception {
	 *   environment
	 *     .includeSystemEnv(false);
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <pre>
	 * &lt;yarn:environment include-system-env="false"/>
	 * </pre>
	 *
	 * @param includeSystemEnv if system env variables should be included
	 * @return {@link YarnEnvironmentConfigurer} for chaining
	 */
	YarnEnvironmentConfigurer includeSystemEnv(boolean includeSystemEnv);

	/**
	 * Specify properties with a {@link org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigurer}.
	 *
	 * <p>JavaConfig:
	 * <pre>
	 * public void configure(YarnEnvironmentConfigure environment) throws Exception {
	 *   Properties props = new Properties();
	 *   environment
	 *     .withProperties()
	 *       .properties(props)
	 *       .property("myKey1", ",myValue1")
	 *       .and();
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <pre>
	 * &lt;util:properties id="props" location="props.properties"/>
	 *   <prop key="myKey1">myValue1</prop>
	 * &lt;/util:properties>
	 * &lt;yarn:environment properties-ref="props"/>
	 * </pre>
	 *
	 * @return {@link org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigurer} for chaining
	 * @throws Exception if error occurred
	 */
	PropertiesConfigurer<YarnEnvironmentConfigurer> withProperties() throws Exception;

}
