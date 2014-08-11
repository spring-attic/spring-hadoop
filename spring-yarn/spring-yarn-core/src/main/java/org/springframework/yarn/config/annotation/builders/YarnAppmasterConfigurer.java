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
import org.springframework.yarn.config.annotation.configurers.DefaultMasterContainerAllocatorConfigurer;
import org.springframework.yarn.config.annotation.configurers.DefaultMasterContainerRunnerConfigurer;
import org.springframework.yarn.config.annotation.configurers.MasterContainerAllocatorConfigurer;
import org.springframework.yarn.config.annotation.configurers.MasterContainerRunnerConfigurer;

/**
 * {@code YarnAppmasterConfigure} is an interface for {@code YarnAppmasterBuilder} which is
 * exposed to user via {@link SpringYarnConfigurerAdapter}.
 * <p>
 * Typically configuration is shown below.
 * <br>
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
	 * <br>
	 * <br>JavaConfig:
	 * <br>
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
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;util:properties id="arguments"&gt;
	 *   &lt;prop key="foo1"&gt;bar1&lt;/prop&gt;
	 *   &lt;prop key="foo2"&gt;bar2&lt;/prop&gt;
	 * &lt;/util:properties&gt;
	 *
	 * &lt;yarn:master&gt;
	 *   &lt;yarn:container-runner arguments="arguments"/&gt;
	 * &lt;/yarn:master&gt;
	 * </pre>
	 *
	 * @return {@link MasterContainerRunnerConfigurer} for chaining
	 * @throws Exception exception
	 */
	MasterContainerRunnerConfigurer withContainerRunner() throws Exception;

	/**
	 * Specify a container allocator for Appmaster. Applies a new
	 * {@link DefaultMasterContainerAllocatorConfigurer} into a current builder.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .withContainerAllocator()
	 *       .priority(0)
	 *       .virtualCores(1)
	 *       .memory(1024);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:master&gt;
	 *   &lt;yarn:container-allocator priority="0" virtualcores="1" memory="1024"/&gt;
	 * &lt;/yarn:master&gt;
	 * </pre>
	 *
	 * @return {@link MasterContainerAllocatorConfigurer} for chaining
	 * @throws Exception exception
	 */
	MasterContainerAllocatorConfigurer withContainerAllocator() throws Exception;

	/**
	 * Specify a raw array of commands used to start a container.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .containerCommands("date", "1&gt;&lt;LOG_DIR&gt;/Container.stdout", "2&gt;&lt;LOG_DIR&gt;/Container.stderr");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:master&gt;
	 *   &lt;yarn:container-command&gt;
	 *     &lt;![CDATA[
	 *     date
	 *     1&gt;&lt;LOG_DIR&gt;/Container.stdout
	 *     2&gt;&lt;LOG_DIR&gt;/Container.stderr
	 *     ]]&gt;
	 *   &lt;/yarn:container-command&gt;
	 * &lt;/yarn:master&gt;
	 * </pre>
	 *
	 * @param commands The Yarn container commands
	 * @return {@link YarnAppmasterConfigurer} for chaining
	 */
	YarnAppmasterConfigurer containerCommands(String[] commands);

	/**
	 * Specify a raw array of commands used to start a container. This method
	 * also allows to associate commands with an identifier which is used
	 * for example with container groups where different commands are used.
	 *
	 * @param id the commands identifier
	 * @param commands The Yarn container commands
	 * @return {@link YarnAppmasterConfigurer} for chaining
	 * @see #containerCommands(String[])
	 */
	YarnAppmasterConfigurer containerCommands(String id, String[] commands);

	/**
	 * Specify a {@code YarnAppmaster} class.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .appmasterClass(MyYarnAppmaster.class);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:master appmaster-class="com.example.MyYarnAppmaster"/&gt;
	 * </pre>
	 *
	 * @param clazz The Yarn appmaster class
	 * @return {@link YarnAppmasterConfigurer} for chaining
	 */
	YarnAppmasterConfigurer appmasterClass(Class<? extends YarnAppmaster> clazz);

	/**
	 * Specify a {@code YarnAppmaster} as a fully qualified class name.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .appmasterClass(MyYarnAppmaster.class);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param clazz The Yarn appmaster class
	 * @return {@link YarnAppmasterConfigurer} for chaining
	 */
	YarnAppmasterConfigurer appmasterClass(String clazz);

}
