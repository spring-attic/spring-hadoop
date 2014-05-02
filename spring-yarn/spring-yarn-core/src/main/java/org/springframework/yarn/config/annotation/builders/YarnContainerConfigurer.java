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
import org.springframework.yarn.container.YarnContainer;

/**
 * {@code YarnContainerConfigure} is an interface for {@code YarnContainerBuilder} which is
 * exposed to user via {@link SpringYarnConfigurerAdapter}.
 * <p>
 * Typically configuration is shown below.
 * <br>
 * <pre>
 * &#064;Configuration
 * &#064;EnableYarn(enable=Enable.CONTAINER)
 * static class Config extends SpringYarnConfigurerAdapter {
 *
 *   &#064;Override
 *   public void configure(YarnContainerConfigure container) throws Exception {
 *     container
 *       .containerClass(MyYarnContainer.class);
 *   }
 *
 * }
 * </pre>
 *
 * @author Janne Valkealahti
 *
 */
public interface YarnContainerConfigurer {

	/**
	 * Specify a {@code YarnContainer} class.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * public void configure(YarnContainerConfigure container) throws Exception {
	 *   container
	 *     .containerClass(MyYarnContainer.class);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;yarn:container container-class="com.example.MyYarnContainer"/&gt;
	 * </pre>
	 *
	 * @param clazz The Yarn container class
	 * @return {@link YarnContainerConfigurer} for chaining
	 */
	YarnContainerConfigurer containerClass(Class<? extends YarnContainer> clazz);

	/**
	 * Specify a {@code YarnContainer} as a fully qualified class name.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * public void configure(YarnContainerConfigure container) throws Exception {
	 *   container
	 *     .containerClass("foo.example.MyYarnContainer");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param clazz The Yarn container class
	 * @return {@link YarnContainerConfigurer} for chaining
	 */
	YarnContainerConfigurer containerClass(String clazz);

	/**
	 * Specify a {@code YarnContainer} reference.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 * &#064;Autowired
	 * private YarnContainer yarnContainer;
	 *
	 * public void configure(YarnContainerConfigure container) throws Exception {
	 *   container
	 *     .containerRef(MyYarnContainer.class);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * <pre>
	 * &lt;bean id="myYarnContainer" class="com.example.MyYarnContainer"/&gt;
	 * &lt;yarn:container container-ref="myYarnContainer"/&gt;
	 * </pre>
	 *
	 * @param ref The Yarn container reference
	 * @return {@link YarnContainerConfigurer} for chaining
	 */
	YarnContainerConfigurer containerRef(YarnContainer ref);

}
