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
package org.springframework.yarn.boot.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Configuration;

/**
 * Generic Spring Boot {@link SpringApplication} class which
 * can be used as a main class if only requirement from an application
 * is to pass arguments into {@link SpringApplication#run(Object, String...)}
 * <p>
 * Usual use case for this would be to define this class as
 * <code>Main-Class</code> when creating i.e. executable jars
 * using Spring Boot maven or gradle plugins. User can always create
 * a similar dummy main class within a packaged application and let
 * Spring Boot maven or gradle plugin to find it during the creating
 * of an executable jar.
 * <p>
 * Care must be taken into account that if used, this class
 * will enable a system with @{@link EnableAutoConfiguration}. If
 * there is a need to exclude any automatic auto-configuration, user
 * should define a custom class.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@EnableAutoConfiguration
public class SpringYarnBootApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringYarnBootApplication.class, args);
	}

}
