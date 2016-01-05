/*
 * Copyright 2013-2016 the original author or authors.
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
 * <br>
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
 * <br>XML:
 * <pre>
 * &lt;yarn:client app-name="myAppName"&gt;
 *   &lt;yarn:master-runner /&gt;
 * &lt;/yarn:client&gt;
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
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .withMasterRunner()
	 *       .contextClass(MyAppmasterConfiguration.class);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:client&gt;
	 *   &lt;yarn:master-runner /&gt;
	 * &lt;/yarn:client&gt;
	 * </pre>
	 *
	 * @return {@link ClientMasterRunnerConfigurer} for chaining
	 * @throws Exception exception
	 */
	ClientMasterRunnerConfigurer withMasterRunner() throws Exception;

	/**
	 * Specify a yarn application name.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .appName("myAppName");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:client app-name="myAppName"/&gt;
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
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .appType("BOOT");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param appType The Yarn application type
	 * @return {@link YarnClientConfigurer} for chaining
	 */
	YarnClientConfigurer appType(String appType);

	/**
	 * Specify a raw array of commands used to start an application master.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .masterCommands("java -jar MyApp.jar", "1&gt;&lt;LOG_DIR&gt;/Appmaster.stdout", "2&gt;&lt;LOG_DIR&gt;/Appmaster.stderr");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:client&gt;
	 *   &lt;yarn:master-command&gt;
	 *     &lt;![CDATA[
	 *     java -jar MyApp.jar
	 *     1&gt;&lt;LOG_DIR&gt;/Appmaster.stdout
	 *     2&gt;&lt;LOG_DIR&gt;/Appmaster.stderr
	 *     ]]&gt;
	 *   &lt;/yarn:master-command&gt;
	 * &lt;/yarn:client&gt;
	 * </pre>
	 *
	 * @param commands The Yarn container commands
	 * @return {@link YarnAppmasterConfigurer} for chaining
	 */
	YarnClientConfigurer masterCommands(String... commands);

	/**
	 * Specify a yarn application priority.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .priority(0);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:client priority="0"/&gt;
	 * </pre>
	 *
	 * @param priority The Yarn application priority
	 * @return {@link YarnClientConfigurer} for chaining
	 */
	YarnClientConfigurer priority(Integer priority);

	/**
	 * Specify a yarn application virtual core resource count.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .virtualCores(1);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:client virtualcores="1"/&gt;
	 * </pre>
	 *
	 * @param virtualCores The Yarn application virtual core resource count
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
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .memory("1G");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:client memory="1024"/&gt;
	 * </pre>
	 *
	 * @param memory The Yarn application containers memory reservation
	 * @return {@link YarnClientConfigurer} for chaining
	 */
	YarnClientConfigurer memory(String memory);

	/**
	 * Specify a yarn application containers memory reservation.
	 * The <code>memory</code> argument is given as MegaBytes.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .memory(1024);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:client memory="1024"/&gt;
	 * </pre>
	 *
	 * @param memory The Yarn application containers memory reservation
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
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .queue("default");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:client queue="default"/&gt;
	 * </pre>
	 *
	 * @param queue The Yarn application submission queue
	 * @return {@link YarnClientConfigurer} for chaining
	 */
	YarnClientConfigurer queue(String queue);

	/**
	 * Specify a yarn application submission label expression. If this is set,
	 * all containers of this application without setting label expression
	 * in resource request will get allocated resources on only those nodes that
	 * satisfy this label expression. If different label expression of this app
	 * and resource request are set at the same time, the one set in resource
	 * request will be used when allocating container.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .labelExpression("expression");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param labelExpression The Yarn application label expression
	 * @return {@link YarnClientConfigurer} for chaining
	 */
	YarnClientConfigurer labelExpression(String labelExpression);

	/**
	 * Specify a {@code YarnClient} class.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .clientClass(MyYarnClient.class);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param clazz The Yarn client class
	 * @return {@link YarnClientConfigurer} for chaining
	 */
	YarnClientConfigurer clientClass(Class<? extends YarnClient> clazz);

	/**
	 * Specify a {@code YarnClient} as a fully qualified class name.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * public void configure(YarnClientConfigure client) throws Exception {
	 *   client
	 *     .clientClass("com.example.MyYarnClient");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param clazz The Yarn client class
	 * @return {@link YarnClientConfigurer} for chaining
	 */
	YarnClientConfigurer clientClass(String clazz);

}
