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
 * <br>
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
	 * <br>JavaConfig:
	 * <pre>
	 * public void configure(YarnEnvironmentBuilder environment) throws Exception {
	 *   environment
	 *     .withClasspath()
	 *       .entry("cpEntry1")
	 *       .entry("cpEntry2")
	 *       .useDefaultYarnClasspath(true);
	 * }
	 * </pre>
	 * <br>XML:
	 * <pre>
	 * &lt;yarn:environment&gt;
	 *   &lt;yarn:classpath use-yarn-app-classpath="true" delimiter=":"&gt;
	 *     cpEntry1
	 *     cpEntry2
	 *   &lt;/yarn:classpath&gt;
	 * &lt;/yarn:environment&gt;
	 * </pre>
	 *
	 * @return {@link EnvironmentClasspathConfigurer} for classpath
	 * @throws Exception if error occurred
	 */
	EnvironmentClasspathConfigurer withClasspath() throws Exception;

	/**
	 * Specify a classpath environment variable using an identifier.
	 *
	 * @param id the identifier
	 * @return {@link EnvironmentClasspathConfigurer} for classpath
	 * @throws Exception if error occurred
	 * @see #withClasspath()
	 */
	EnvironmentClasspathConfigurer withClasspath(String id) throws Exception;

	/**
	 * Specify an environment variable.
	 *
	 * <br>JavaConfig:
	 * <pre>
	 * public void configure(YarnEnvironmentConfigure environment) throws Exception {
	 *   environment
	 *     .entry("myKey1","myValue1")
	 *     .entry("myKey2","myValue2");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <pre>
	 * &lt;yarn:environment&gt;
	 *   myKey1=myValue1
	 *   myKey2=myValue2
	 * &lt;/yarn:environment&gt;
	 * </pre>
	 *
	 * @param key The environment key
	 * @param value The environment value
	 * @return {@link YarnEnvironmentConfigurer} for chaining
	 */
	YarnEnvironmentConfigurer entry(String key, String value);

	/**
	 * Specify an environment variable using an identifier.
	 *
	 * @param id the identifier
	 * @param key The environment key
	 * @param value The environment value
	 * @return {@link YarnEnvironmentConfigurer} for chaining
	 * @see #entry(String, String)
	 */
	YarnEnvironmentConfigurer entry(String id, String key, String value);

	/**
	 * Specify properties locations.
	 *
	 * <br>JavaConfig:
	 * <pre>
	 * public void configure(YarnEnvironmentConfigure environment) throws Exception {
	 *   environment
	 *     .entry("myKey1","myValue1")
	 *     .entry("myKey2","myValue2")
	 *     .propertiesLocation("cfg-1.properties", "cfg-2.properties");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <pre>
	 * &lt;yarn:environment properties-location="cfg-1.properties, cfg-2.properties"&gt;
	 *   myKey1=myValue1
	 *   myKey2=myValue2
	 * &lt;/yarn:environment&gt;
	 * </pre>
	 *
	 * @param locations The properties file locations
	 * @return {@link YarnEnvironmentConfigurer} for chaining
	 * @throws IOException if error occurred
	 */
	YarnEnvironmentConfigurer propertiesLocation(String... locations) throws IOException;

	/**
	 * Specify properties locations with an identifier.
	 *
	 * @param id the identifier
	 * @param locations the properties file locations
	 * @return {@link YarnEnvironmentConfigurer} for chaining
	 * @throws IOException if error occurred
	 * @see #propertiesLocation(String...)
	 */
	YarnEnvironmentConfigurer propertiesLocationId(String id, String[] locations) throws IOException;

	/**
	 * Specify if existing system environment variables should
	 * be included automatically.
	 *
	 * <br>JavaConfig:
	 * <pre>
	 * public void configure(YarnEnvironmentConfigure environment) throws Exception {
	 *   environment
	 *     .includeLocalSystemEnv(false);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <pre>
	 * &lt;yarn:environment include-local-system-env="false"/&gt;
	 * </pre>
	 *
	 * @param includeLocalSystemEnv if system env variables should be included
	 * @return {@link YarnEnvironmentConfigurer} for chaining
	 */
	YarnEnvironmentConfigurer includeLocalSystemEnv(boolean includeLocalSystemEnv);

	/**
	 * Specify if existing system environment variables should
	 * be included automatically with an identifier.
	 *
	 * @param id the identifier
	 * @param includeLocalSystemEnv if system env variables should be included
	 * @return {@link YarnEnvironmentConfigurer} for chaining
	 * @see #includeLocalSystemEnv(boolean)
	 */
	YarnEnvironmentConfigurer includeLocalSystemEnv(String id, boolean includeLocalSystemEnv);

	/**
	 * Specify properties with a {@link org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigurer}.
	 *
	 * <br>JavaConfig:
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
	 * <br>XML:
	 * <pre>
	 * &lt;util:properties id="props" location="props.properties"/&gt;
	 *   &lt;prop key="myKey1"&gt;myValue1&lt;/prop&gt;
	 * &lt;/util:properties&gt;
	 * &lt;yarn:environment properties-ref="props"/&gt;
	 * </pre>
	 *
	 * @return {@link org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigurer} for chaining
	 * @throws Exception if error occurred
	 */
	PropertiesConfigurer<YarnEnvironmentConfigurer> withProperties() throws Exception;

	/**
	 * Specify properties with a {@link org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigurer}
	 * with an identifier.
	 *
	 * @param id the identifier
	 * @return {@link org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigurer} for chaining
	 * @throws Exception if error occurred
	 * @see #withProperties()
	 */
	PropertiesConfigurer<YarnEnvironmentConfigurer> withProperties(String id) throws Exception;

}
