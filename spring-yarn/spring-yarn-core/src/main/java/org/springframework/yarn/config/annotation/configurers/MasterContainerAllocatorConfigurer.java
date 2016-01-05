/*
 * Copyright 2014-2016 the original author or authors.
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
import org.springframework.yarn.config.annotation.configurers.DefaultMasterContainerAllocatorConfigurer.DefaultMasterContainerAllocatorCollectionConfigurer;

/**
 * {@link AnnotationConfigurerBuilder} for configuring {@link ContainerAllocator}.
 *
 * <br>
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
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .withContainerAllocator()
	 *       .priority(0);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:master&gt;
	 *   &lt;yarn:container-allocator priority="0"/&gt;
	 * &lt;/yarn:master&gt;
	 * </pre>
	 *
	 * @param priority the priority
	 * @return {@link MasterContainerAllocatorConfigurer} for chaining
	 */
	MasterContainerAllocatorConfigurer priority(Integer priority);

	/**
	 * Specify a container label expression for {@link ContainerAllocator}.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .withContainerAllocator()
	 *       .labelExpression("expression");
	 * }
	 * </pre>
	 *
	 * @param labelExpression the label expression
	 * @return {@link MasterContainerAllocatorConfigurer} for chaining
	 */
	MasterContainerAllocatorConfigurer labelExpression(String labelExpression);

	/**
	 * Specify a container virtual cores for {@link ContainerAllocator}.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .withContainerAllocator()
	 *       .virtualCores(1);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:master&gt;
	 *   &lt;yarn:container-allocator virtualcores="1"/&gt;
	 * &lt;/yarn:master&gt;
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
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .withContainerAllocator()
	 *       .memory("1G");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:master&gt;
	 *   &lt;yarn:container-allocator memory="1024"/&gt;
	 * &lt;/yarn:master&gt;
	 * </pre>
	 *
	 * @param memory the memory
	 * @return {@link MasterContainerAllocatorConfigurer} for chaining
	 */
	MasterContainerAllocatorConfigurer memory(String memory);

	/**
	 * Specify a container memory for {@link ContainerAllocator}.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .withContainerAllocator()
	 *       .memory(1024);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:master&gt;
	 *   &lt;yarn:container-allocator memory="1024"/&gt;
	 * &lt;/yarn:master&gt;
	 * </pre>
	 *
	 * @param memory the memory
	 * @return {@link MasterContainerAllocatorConfigurer} for chaining
	 * @see #memory(String)
	 */
	MasterContainerAllocatorConfigurer memory(int memory);

	/**
	 * Specify a locality relaxing for {@link ContainerAllocator}. Setting
	 * this flag <code>true</code> means that resource requests will
	 * not use locality relaxing. Default for this flag is <code>false</code>.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .withContainerAllocator()
	 *       .locality(false);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:master&gt;
	 *   &lt;yarn:container-allocator locality="false"/&gt;
	 * &lt;/yarn:master&gt;
	 * </pre>
	 *
	 * @param locality the locality flag for resource relaxing
	 * @return {@link MasterContainerAllocatorConfigurer} for chaining
	 */
	MasterContainerAllocatorConfigurer locality(boolean locality);

	/**
	 * Specify a collection of container allocator attributes. Applies a new
	 * {@link DefaultMasterContainerAllocatorCollectionConfigurer} into a current configurer.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(YarnAppmasterConfigure master) throws Exception {
	 *   master
	 *     .withContainerAllocator()
	 *       .withCollection("id")
	 *         .priority(0);
	 * }
	 * </pre>
	 *
	 * @param id the id
	 * @return {@link MasterContainerAllocatorCollectionConfigurer} for chaining
	 */
	MasterContainerAllocatorCollectionConfigurer withCollection(String id);

	/**
	 * {@code MasterContainerAllocatorCollectionConfigurer} is an interface
	 * for {@code DefaultMasterContainerAllocatorCollectionConfigurer} which is
	 * used to configure {@link MasterContainerAllocatorConfigurer} parameters
	 * as an identified collection.
	 */
	public interface MasterContainerAllocatorCollectionConfigurer {

		/**
		 * Specify a container priority for {@link ContainerAllocator}.
		 *
		 * @param priority the priority
		 * @return {@link MasterContainerAllocatorCollectionConfigurer} for chaining
		 * @see MasterContainerAllocatorConfigurer#priority(Integer)
		 */
		MasterContainerAllocatorCollectionConfigurer priority(Integer priority);

		/**
		 * Specify a container label expression for {@link ContainerAllocator}.
		 *
		 * @param labelExpression the label expression
		 * @return {@link MasterContainerAllocatorCollectionConfigurer} for chaining
		 * @see MasterContainerAllocatorConfigurer#labelExpression(String)
		 */
		MasterContainerAllocatorCollectionConfigurer labelExpression(String labelExpression);

		/**
		 * Specify a container virtual cores for {@link ContainerAllocator}.
		 *
		 * @param virtualCores the virtual cores
		 * @return {@link MasterContainerAllocatorCollectionConfigurer} for chaining
		 * @see MasterContainerAllocatorConfigurer#virtualCores(Integer)
		 */
		MasterContainerAllocatorCollectionConfigurer virtualCores(Integer virtualCores);

		/**
		 * Specify a container memory for {@link ContainerAllocator}.
		 *
		 * @param memory the memory
		 * @return {@link MasterContainerAllocatorCollectionConfigurer} for chaining
		 * @see MasterContainerAllocatorConfigurer#memory(String)
		 */
		MasterContainerAllocatorCollectionConfigurer memory(String memory);

		/**
		 * Specify a container memory for {@link ContainerAllocator}.
		 *
		 * @param memory the memory
		 * @return {@link MasterContainerAllocatorCollectionConfigurer} for chaining
		 * @see MasterContainerAllocatorConfigurer#memory(String)
		 */
		MasterContainerAllocatorCollectionConfigurer memory(int memory);

		/**
		 * Specify a locality relaxing for {@link ContainerAllocator}.
		 *
		 * @param locality the locality flag for resource relaxing
		 * @return {@link MasterContainerAllocatorCollectionConfigurer} for chaining
		 * @see MasterContainerAllocatorConfigurer#locality(boolean)
		 */
		MasterContainerAllocatorCollectionConfigurer locality(boolean locality);

		/**
		 * Returns a parent {@link MasterContainerAllocatorConfigurer} configurer.
		 *
		 * @return {@link MasterContainerAllocatorConfigurer} for chaining
		 */
		MasterContainerAllocatorConfigurer and();

	}

}
