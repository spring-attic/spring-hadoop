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

import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.config.annotation.SpringYarnConfigurerAdapter;
import org.springframework.yarn.config.annotation.configurers.ClientMasterRunnerConfigurer;
import org.springframework.yarn.config.annotation.configurers.DefaultClientMasterRunnerConfigurer;

/**
 * {@code YarnClientConfigure} is an interface for {@code YarnClientBuilder} which is
 * exposed to user via {@link SpringYarnConfigurerAdapter}.
 * <p>
 * Typically configuration is shown below.
 * <p>
 * <pre>
 * &#064;Configuration
 * &#064;EnableYarn(enable=Enable.CLIENT)
 * static class Config extends SpringYarnConfigurerAdapter {
 *
 *   &#064;Override
 *   public void configure(YarnClientConfigure client) throws Exception {
 *     client
 *       .appName("myAppName")
 *       .withMasterRunner()
 *         .contextClass(MyAppmasterConfiguration.class);
 *   }
 *
 * }
 * </pre>
 * <p>XML:
 * <pre>
 * &lt;yarn:client app-name="myAppName">
 *   &lt;yarn:master-runner />
 * &lt;/yarn:client>
 * </pre>
 *
 * @author Janne Valkealahti
 *
 */
public interface YarnClientConfigurer {

	/**
	 * Specify a runner for Appmaster. Applies a new {@link DefaultClientMasterRunnerConfigurer}
	 * into current builder.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .withMasterRunner()
	 *       .contextClass(MyAppmasterConfiguration.class);
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * <pre>
	 * &lt;yarn:client>
	 *   &lt;yarn:master-runner />
	 * &lt;/yarn:client>
	 * </pre>
	 *
	 * @return {@link ClientMasterRunnerConfigurer} for chaining
	 */
	ClientMasterRunnerConfigurer withMasterRunner() throws Exception;

	/**
	 * Specify a yarn application name.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .appName("myAppName");
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * <pre>
	 * &lt;yarn:client app-name="myAppName"/>
	 * </pre>
	 *
	 * @param appName The Yarn application name
	 * @return {@link YarnClientConfigurer} for chaining
	 */
	YarnClientConfigurer appName(String appName);

	/**
	 * Specify a yarn application type. Type is a simple string user will see
	 * as a field when querying applications from a resource manager. For
	 * example, MapReduce jobs are using type <code>MAPREDUCE</code> and other
	 * applications defaults to <code>YARN</code>.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .appType("BOOT");
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * No equivalent
	 *
	 * @param appName The Yarn application type
	 * @return {@link YarnClientConfigurer} for chaining
	 */
	YarnClientConfigurer appType(String appType);

	/**
	 * Specify a raw array of commands used to start an application master.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .masterCommands("java -jar MyApp.jar", "1><LOG_DIR>/Appmaster.stdout", "2><LOG_DIR>/Appmaster.stderr");
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * <pre>
	 * &lt;yarn:client>
	 *   &lt;yarn:master-command>
	 *     &lt;![CDATA[
	 *     java -jar MyApp.jar
	 *     1><LOG_DIR>/Appmaster.stdout
	 *     2><LOG_DIR>/Appmaster.stderr
	 *     ]]>
	 *   &lt;/yarn:master-command>
	 * &lt;/yarn:client>
	 * </pre>
	 *
	 * @param commands The Yarn container commands
	 * @return {@link YarnAppmasterConfigurer} for chaining
	 */
	YarnClientConfigurer masterCommands(String... commands);

	/**
	 * Specify a yarn application priority.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .priority(0);
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * <pre>
	 * &lt;yarn:client priority="0"/>
	 * </pre>
	 *
	 * @param priority The Yarn application priority
	 * @return {@link YarnClientConfigurer} for chaining
	 */
	YarnClientConfigurer priority(Integer priority);

	/**
	 * Specify a yarn application virtual core resource count.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .virtualCores(1);
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * <pre>
	 * &lt;yarn:client virtualcores="1"/>
	 * </pre>
	 *
	 * @param priority The Yarn application virtual core resource count
	 * @return {@link YarnClientConfigurer} for chaining
	 */
	YarnClientConfigurer virtualCores(Integer virtualCores);

	/**
	 * Specify a yarn application containers memory reservation.
	 * The <code>memory</code> argument is given as MegaBytes if
	 * value is a plain number. Shortcuts like <code>1G</code> and
	 * <code>500M</code> can be used which translates to <code>1024</code>
	 * and <code>500</code> respectively.
	 * <p>
	 * This method is equivalent to {@code #memory(int)} so that
	 * argument can be given as a {@code String}.
	 * <p>
	 * <b>NOTE:</b> be careful not to use a too low settings like
	 * <code>1000K</code> or <code>1000B</code> because those are rounded
	 * down to full <code>MB</code>s and thus becomes a zero. Also too
	 * high values may make resource allocation to behave badly.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .memory("1G");
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * <pre>
	 * &lt;yarn:client memory="1024"/>
	 * </pre>
	 *
	 * @param priority The Yarn application containers memory reservation
	 * @return {@link YarnClientConfigurer} for chaining
	 */
	YarnClientConfigurer memory(String memory);

	/**
	 * Specify a yarn application containers memory reservation.
	 * The <code>memory</code> argument is given as MegaBytes.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .memory(1024);
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * <pre>
	 * &lt;yarn:client memory="1024"/>
	 * </pre>
	 *
	 * @param priority The Yarn application containers memory reservation
	 * @return {@link YarnClientConfigurer} for chaining
	 * @see #memory(String)
	 */
	YarnClientConfigurer memory(int memory);

	/**
	 * Specify a yarn application submission queue. Specified queue
	 * is a one client requests but it's not necessarily a one
	 * where application is placed. Some Yarn schedulers may choose
	 * to change this so user should be aware of how Yarn is setup.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .queue("default");
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * <pre>
	 * &lt;yarn:client queue="default"/>
	 * </pre>
	 *
	 * @param priority The Yarn application submission queue
	 * @return {@link YarnClientConfigurer} for chaining
	 */
	YarnClientConfigurer queue(String queue);

	/**
	 * Specify a {@code YarnClient} class.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .clientClass(MyYarnClient.class);
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * No equivalent
	 *
	 * @param clazz The Yarn client class
	 * @return {@link YarnClientConfigurer} for chaining
	 */
	YarnClientConfigurer clientClass(Class<? extends YarnClient> clazz);

	/**
	 * Specify a {@code YarnClient} as a fully qualified class name.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .clientClass("com.example.MyYarnClient");
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * No equivalent
	 *
	 * @param clazz The Yarn client class
	 * @return {@link YarnClientConfigurer} for chaining
	 */
	YarnClientConfigurer clientClass(String clazz);

}
