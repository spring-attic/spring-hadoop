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

import org.springframework.yarn.config.annotation.SpringYarnConfigurerAdapter;
import org.springframework.yarn.config.annotation.configurers.ClientMasterRunnerConfigure;
import org.springframework.yarn.config.annotation.configurers.ClientMasterRunnerConfigurer;

/**
 * {@code YarnClientConfigure} is an interface for {@code YarnClientBuilder} which is
 * exposed to user via {@link SpringYarnConfigurerAdapter}.
 * <p>
 * Typically configuration is shown below.
 * <p>
 * <pre>
 * &#064;Configuration
 * &#064;EnableYarn
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
public interface YarnClientConfigure {

	/**
	 * Specify a runner for Appmaster. Applies a new {@link ClientMasterRunnerConfigurer}
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
	 * @return {@link ClientMasterRunnerConfigure} for chaining
	 */
	ClientMasterRunnerConfigure withMasterRunner() throws Exception;

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
	 * @return {@link YarnClientConfigure} for chaining
	 */
	YarnClientConfigure appName(String appName);

	YarnClientConfigure appType(String appType);

	YarnClientConfigure masterCommands(String... commands);

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
	 * @return {@link YarnClientConfigure} for chaining
	 */
	YarnClientConfigure priority(Integer priority);

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
	 * @return {@link YarnClientConfigure} for chaining
	 */
	YarnClientConfigure virtualCores(Integer virtualCores);

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
	 * @return {@link YarnClientConfigure} for chaining
	 */
	YarnClientConfigure memory(String memory);

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
	 * @return {@link YarnClientConfigure} for chaining
	 * @see #memory(String)
	 */
	YarnClientConfigure memory(int memory);

	/**
	 * Specify a yarn application submission queue.
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
	 * @return {@link YarnClientConfigure} for chaining
	 */
	YarnClientConfigure queue(String queue);

}
