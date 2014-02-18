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

import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurerBuilder;
import org.springframework.yarn.am.allocate.ContainerAllocator;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterConfigurer;

/**
 * {@link AnnotationConfigurerBuilder} for configuring {@link ContainerAllocator}.
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
 *   public void configure(YarnAppmasterConfigure master) throws Exception {
 *     master
 *       .withContainerAllocator();
 *   }
 *
 * }
 * </pre>
 *
 * @author Janne Valkealahti
 *
 */
public interface MasterContainerAllocatorConfigurer extends AnnotationConfigurerBuilder<YarnAppmasterConfigurer> {

	/**
	 * Specify a container priority for {@link ContainerAllocator}.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 *
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .withContainerAllocator()
	 *       .priority(0);
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * <pre>
	 * &lt;yarn:master>
	 *   &lt;yarn:container-allocator priority="0"/>
	 * &lt;/yarn:master>
	 * </pre>
	 *
	 * @param priority the priority
	 * @return {@link MasterContainerAllocatorConfigurer} for chaining
	 */
	MasterContainerAllocatorConfigurer priority(Integer priority);

	/**
	 * Specify a container virtual cores for {@link ContainerAllocator}.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 *
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .withContainerAllocator()
	 *       .virtualCores(1);
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * <pre>
	 * &lt;yarn:master>
	 *   &lt;yarn:container-allocator virtualcores="1"/>
	 * &lt;/yarn:master>
	 * </pre>
	 *
	 * @param virtualCores the virtual cores
	 * @return {@link MasterContainerAllocatorConfigurer} for chaining
	 */
	MasterContainerAllocatorConfigurer virtualCores(Integer virtualCores);

	/**
	 * Specify a container memory for {@link ContainerAllocator}.
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
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .withContainerAllocator()
	 *       .memory("1G");
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * <pre>
	 * &lt;yarn:master>
	 *   &lt;yarn:container-allocator memory="1024"/>
	 * &lt;/yarn:master>
	 * </pre>
	 *
	 * @param memory the memory
	 * @return {@link MasterContainerAllocatorConfigurer} for chaining
	 */
	MasterContainerAllocatorConfigurer memory(String memory);

	/**
	 * Specify a container memory for {@link ContainerAllocator}.
	 *
	 * <p>
	 * <p>JavaConfig:
	 * <p>
	 * <pre>
	 *
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .withContainerAllocator()
	 *       .memory(1024);
	 * }
	 * </pre>
	 *
	 * <p>XML:
	 * <p>
	 * <pre>
	 * &lt;yarn:master>
	 *   &lt;yarn:container-allocator memory="1024"/>
	 * &lt;/yarn:master>
	 * </pre>
	 *
	 * @param memory the memory
	 * @return {@link MasterContainerAllocatorConfigurer} for chaining
	 * @see #memory(String)
	 */
	MasterContainerAllocatorConfigurer memory(int memory);

}
