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

import org.apache.hadoop.conf.Configuration;
import org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigurer;
import org.springframework.data.hadoop.config.common.annotation.configurers.ResourceConfigurer;
import org.springframework.data.hadoop.config.common.annotation.configurers.SecurityConfigurer;
import org.springframework.yarn.config.annotation.SpringYarnConfigurerAdapter;

/**
 * {@code YarnConfigConfigure} is an interface for {@code YarnConfigBuilder} which is
 * exposed to user via {@link SpringYarnConfigurerAdapter}.
 * <p>
 * Typically configuration is shown below.
 * <br>
 * <pre>
 * &#064;Configuration
 * &#064;EnableYarn
 * static class Config extends SpringYarnConfigurerAdapter {
 *
 *   &#064;Override
 *   public void configure(YarnConfigConfigure config) throws Exception {
 *     config
 *       .fileSystemUri("hdfs://foo.uri")
 *       .withResources()
 *         .resource("classpath:/test-site-1.xml")
 *         .resource("classpath:/test-site-2.xml")
 *         .and()
 *       .withProperties()
 *         .property("foo", "jee");
 *   }
 *
 * }
 * </pre>
 *
 * @author Janne Valkealahti
 *
 */
public interface YarnConfigConfigurer {

	/**
	 * Specify configuration options as resource properties with a {@link org.springframework.data.hadoop.config.common.annotation.configurers.ResourceConfigurer}.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * public void configure(YarnConfigConfigure config) throws Exception {
	 *   Properties props = new Properties();
	 *   config
	 *     .withResources()
	 *       .resource("cfg-1.properties")
	 *       .resource("cfg-2.properties")
	 *       .and();
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:configuration properties-location="cfg-1.properties, cfg-2.properties"/&gt;
	 * </pre>
	 *
	 * @return {@link org.springframework.data.hadoop.config.common.annotation.configurers.ResourceConfigurer} for chaining
	 * @throws Exception if error occurred
	 */
	ResourceConfigurer<YarnConfigConfigurer> withResources() throws Exception;

	/**
	 * Specify configuration options as properties with a {@link org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigurer}.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * public void configure(YarnConfigConfigure config) throws Exception {
	 *   Properties props = new Properties();
	 *   config
	 *     .withProperties()
	 *       .properties(props)
	 *       .property("myKey1", ",myValue1")
	 *       .and();
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;util:properties id="props" location="props.properties"/&gt;
	 *   &lt;prop key="myKey1"&gt;myValue1&lt;/prop&gt;
	 * &lt;/util:properties&gt;
	 * &lt;yarn:configuration properties-ref="props"/&gt;
	 * </pre>
	 *
	 * @return {@link org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigurer} for chaining
	 * @throws Exception if error occurred
	 */
	PropertiesConfigurer<YarnConfigConfigurer> withProperties() throws Exception;

	/**
	 * Specify security options with a {@link SecurityConfigurer}.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * public void configure(YarnConfigConfigure config) throws Exception {
	 *   config
	 *     .withSecurity()
	 *       .authMethod("kerberos")
	 *       .namenodePrincipal("hdfs/myhost@LOCALDOMAIN")
	 *       .rmManagerPrincipal("yarn/myhost@LOCALDOMAIN");
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <br>
	 * No equivalent
	 *
	 * @return {@link SecurityConfigurer} for chaining
	 * @throws Exception if error occurred
	 */
	SecurityConfigurer<YarnConfigConfigurer> withSecurity() throws Exception;

	/**
	 * Specify a Hdfs file system uri.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * public void configure(YarnConfigConfigure config) throws Exception {
	 *   config
	 *     .fileSystemUri("hdfs://myhost:8020");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:configuration fs-uri="hdfs://myhost:8020"/&gt;
	 * </pre>
	 *
	 * @param uri The Hdfs uri
	 * @return {@link YarnConfigConfigurer} for chaining
	 */
	YarnConfigConfigurer fileSystemUri(String uri);

	/**
	 * Specify a Yarn resource manager address.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * public void configure(YarnConfigConfigure config) throws Exception {
	 *   config
	 *     .resourceManagerAddress("myRmHost:8032");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:configuration rm-address="myRmHost:8032"/&gt;
	 * </pre>
	 *
	 * @param address The Yarn resource manager address
	 * @return {@link YarnConfigConfigurer} for chaining
	 */
	YarnConfigConfigurer resourceManagerAddress(String address);

	/**
	 * Specify a Yarn resource manager scheduler address.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * public void configure(YarnConfigConfigure config) throws Exception {
	 *   config
	 *     .schedulerAddress("myRmHost:8030");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:configuration scheduler-address="myRmHost:8030"/&gt;
	 * </pre>
	 *
	 * @param address The Yarn resource manager scheduler address
	 * @return {@link YarnConfigConfigurer} for chaining
	 */
	YarnConfigConfigurer schedulerAddress(String address);

	/**
	 * Specify if Hadoop {@link Configuration} is initially
	 * based on default values. Default is <code>true</code>.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * public void configure(YarnConfigConfigure config) throws Exception {
	 *   config
	 *     .loadDefaults(true);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param loadDefaults The flag if defaults should be loaded
	 * @return {@link YarnConfigConfigurer} for chaining
	 */
	YarnConfigConfigurer loadDefaults(boolean loadDefaults);

}
