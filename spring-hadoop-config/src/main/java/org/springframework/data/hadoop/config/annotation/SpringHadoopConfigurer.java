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
package org.springframework.data.hadoop.config.annotation;

import org.apache.hadoop.conf.Configuration;
import org.springframework.data.hadoop.config.annotation.builders.HadoopConfigBuilder;
import org.springframework.data.hadoop.config.annotation.builders.HadoopConfigConfigurer;
import org.springframework.data.hadoop.config.annotation.builders.SpringHadoopConfigBuilder;
import org.springframework.data.hadoop.config.common.annotation.AnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurer;

/**
 * Allows for configuring a {@link AnnotationBuilder}. All
 * {@link AnnotationConfigurer} first have their {@link #init(AnnotationBuilder)}
 * method invoked. After all {@link #init(AnnotationBuilder)} methods have been
 * invoked, each {@link #configure(AnnotationBuilder)} method is invoked.
 *
 * @author Janne Valkealahti
 *
 */
public interface SpringHadoopConfigurer extends AnnotationConfigurer<SpringHadoopConfigs, SpringHadoopConfigBuilder> {

	/**
	 * Configure {@link Configuration} via {@link HadoopConfigBuilder} builder.
	 *
	 * @param config the {@link HadoopConfigConfigurer} builder
	 * @throws Exception if error occurred
	 */
	void configure(HadoopConfigConfigurer config) throws Exception;

}
