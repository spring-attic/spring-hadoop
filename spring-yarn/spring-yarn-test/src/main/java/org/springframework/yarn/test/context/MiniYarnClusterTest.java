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
package org.springframework.yarn.test.context;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.yarn.test.YarnTestSystemConstants;

/**
 * Composed annotation having &#064;{@link MiniYarnCluster},
 * &#064;{@link ContextConfiguration} using loader {@link YarnDelegatingSmartContextLoader}
 * and empty Spring &#064;{@link Configuration}.
 * <p>
 * Typical use for this annotation would look like:
 * <p>
 * <pre>
 * &#064;MiniYarnClusterTest
 * public class AppTests extends AbstractBootYarnClusterTests {
 *
 *   &#064;Test
 *   public void testApp() {
 *     // test methods
 *   }
 *
 * }
 * </pre>
 *
 * <p>
 * If user wants to use a simple composed annotation and use a
 * custom &#064;{@link Configuration}, there are two options.
 * <p>
 * Use classes attribute with &#064;{@link MiniYarnCluster} to override
 * default context configuration class.
 * <p>
 * <pre>
 * &#064;MiniYarnClusterTest(classes = AppTests.Config.class)
 * public class AppTests extends AbstractBootYarnClusterTests {
 *
 *   &#064;Test
 *   public void testApp() {
 *     // test methods
 *   }
 *
 *   &#064;Configuration
 *   public static class Config {
 *     // custom config
 *   }
 *
 * }
 * </pre>
 *
 * <p>
 * If more functionality is needed for composed annotation, one can simply duplicate
 * functionality of this &#064;{@code MiniYarnClusterTest} annotation.
 * <p>
 * <pre>
 * &#064;Retention(RetentionPolicy.RUNTIME)
 * &#064;Target(ElementType.TYPE)
 * &#064;ContextConfiguration(loader=YarnDelegatingSmartContextLoader.class)
 * &#064;MiniYarnCluster
 * public &#064;interface CustomMiniYarnClusterTest {
 *
 *   Class<?>[] classes() default { CustomMiniYarnClusterTest.Config.class };
 *
 *   &#064;Configuration
 *   public static class Config {
 *
 *     &#064;Bean
 *     public String myCustomBean() {
 *       return "myCustomBean";
 *     }
 *
 *   }
 *
 * }
 * </pre>
 *
 * @author Janne Valkealahti
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@ContextConfiguration(loader=YarnDelegatingSmartContextLoader.class)
@MiniYarnCluster
public @interface MiniYarnClusterTest {

	@Configuration
	public static class Config {
	}

	/**
	 * @see MiniYarnCluster#configName()
	 */
	String configName() default YarnTestSystemConstants.DEFAULT_ID_MINIYARNCLUSTER_CONFIG;

	/**
	 * @see MiniYarnCluster#clusterName()
	 */
	String clusterName() default YarnTestSystemConstants.DEFAULT_ID_MINIYARNCLUSTER;

	/**
	 * @see MiniYarnCluster#id()
	 */
	String id() default YarnTestSystemConstants.DEFAULT_ID_CLUSTER;

	/**
	 * @see MiniYarnCluster#nodes()
	 */
	int nodes() default 1;

	/**
	 * @see ContextConfiguration#locations()
	 */
	String[] locations() default {};

	/**
	 * Defaults to empty configuration.
	 *
	 * @see ContextConfiguration#classes()
	 */
	Class<?>[] classes() default { MiniYarnClusterTest.Config.class };

	/**
	 * @see ContextConfiguration#initializers()
	 */
	Class<? extends ApplicationContextInitializer<? extends ConfigurableApplicationContext>>[] initializers() default {};

	/**
	 * @see ContextConfiguration#inheritLocations()
	 */
	boolean inheritLocations() default true;

	/**
	 * @see ContextConfiguration#inheritInitializers()
	 */
	boolean inheritInitializers() default true;

	/**
	 * @see ContextConfiguration#name()
	 */
	String name() default "";

}
