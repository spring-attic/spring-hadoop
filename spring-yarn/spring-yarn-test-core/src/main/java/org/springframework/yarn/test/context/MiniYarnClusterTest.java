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

import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;

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
 * Drawback of using a composed annotation like this is that the
 * &#064;{@link Configuration} is then applied from an annotation
 * class itself and user can't no longer add a static &#064;{@link Configuration}
 * class in a test class itself and expect Spring to pick it up from
 * there which is a normal behaviour in Spring testing support.
 * <p>
 * If user wants to use a simple composed annotation and use a
 * custom &#064;{@link Configuration}, one can simply duplicate
 * functionality of this &#064;{@code MiniYarnClusterTest} annotation.
 * <p>
 * <pre>
 * &#064;Retention(RetentionPolicy.RUNTIME)
 * &#064;Target(ElementType.TYPE)
 * &#064;ContextConfiguration(loader=YarnDelegatingSmartContextLoader.class)
 * &#064;MiniYarnCluster
 * public &#064;interface CustomMiniYarnClusterTest {
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

}
