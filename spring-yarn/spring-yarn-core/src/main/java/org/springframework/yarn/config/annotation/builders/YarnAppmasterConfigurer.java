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

import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.config.annotation.SpringYarnConfigurerAdapter;
import org.springframework.yarn.config.annotation.configurers.ClientMasterRunnerConfigurer;
import org.springframework.yarn.config.annotation.configurers.MasterContainerRunnerConfigurer;
import org.springframework.yarn.config.annotation.configurers.DefaultMasterContainerRunnerConfigurer;

/**
 * {@code YarnAppmasterConfigure} is an interface for {@code YarnAppmasterBuilder} which is
 * exposed to user via {@link SpringYarnConfigurerAdapter}.
 * <p>
 * Typically configuration is shown below.
 * <p>
 * <pre>
 * &#064;Configuration
 * &#064;EnableYarn(enable=Enable.APPMASTER)
 * static class Config extends SpringYarnConfigurerAdapter {
 *
 *   &#064;Override
 *   public void configure(YarnAppmasterConfigure master) throws Exception {
 *     master
 *       .appmasterClass(MyAppmaster.class)
 *       .withContainerRunner();
 *   }
 *
 * }
 * </pre>
 *
 * @author Janne Valkealahti
 *
 */
public interface YarnAppmasterConfigurer {

	/**
	 * Specify a container runner for Appmaster. Applies a new
	 * {@link DefaultMasterContainerRunnerConfigurer} into a current builder.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 *
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   Properties properties = new Properties();
	 *   properties.setProperty("foo1", "bar1");
	 *   master
	 *     .withContainerRunner()
	 *       .arguments(properties)
	 *       .argument("foo2", "bar2");
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * <pre>
	 * &lt;util:properties id="arguments">
	 *   &lt;prop key="foo1">bar1&lt;/prop>
	 *   &lt;prop key="foo2">bar2&lt;/prop>
	 * &lt;/util:properties>
	 *
	 * &lt;yarn:master>
	 *   &lt;yarn:container-runner arguments="arguments"/>
	 * &lt;/yarn:master>
	 * </pre>
	 *
	 * @return {@link ClientMasterRunnerConfigurer} for chaining
	 */
	MasterContainerRunnerConfigurer withContainerRunner() throws Exception;

	/**
	 * Specify a raw array of commands used to start a container.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .containerCommands("date", "1><LOG_DIR>/Container.stdout", "2><LOG_DIR>/Container.stderr");
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * <pre>
	 * &lt;yarn:master>
	 *   &lt;yarn:container-command>
	 *     &lt;![CDATA[
	 *     date
	 *     1><LOG_DIR>/Container.stdout
	 *     2><LOG_DIR>/Container.stderr
	 *     ]]>
	 *   &lt;/yarn:container-command>
	 * &lt;/yarn:master>
	 * </pre>
	 *
	 * @param commands The Yarn container commands
	 * @return {@link YarnAppmasterConfigurer} for chaining
	 */
	YarnAppmasterConfigurer containerCommands(String... commands);

	/**
	 * Specify a {@code YarnAppmaster} class.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .appmasterClass(MyYarnAppmaster.class);
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * <pre>
	 * &lt;yarn:master appmaster-class="com.example.MyYarnAppmaster"/>
	 * </pre>
	 *
	 * @param uri The Yarn appmaster class
	 * @return {@link YarnAppmasterConfigurer} for chaining
	 */
	YarnAppmasterConfigurer appmasterClass(Class<? extends YarnAppmaster> clazz);

	/**
	 * Specify a {@code YarnAppmaster} as a fully qualified class name.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .appmasterClass(MyYarnAppmaster.class);
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * No equivalent
	 *
	 * @param uri The Yarn appmaster class
	 * @return {@link YarnAppmasterConfigurer} for chaining
	 */
	YarnAppmasterConfigurer appmasterClass(String clazz);

}
