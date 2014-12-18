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
package org.springframework.data.hadoop.store.config.annotation.configurers;

import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurerBuilder;
import org.springframework.data.hadoop.store.config.annotation.builders.DataStoreTextWriterConfigurer;

/**
 * {@link AnnotationConfigurerBuilder} for configuring a naming strategy.
 *
 * <p>
 * Typically configuration is shown below.
 * <br>
 * <pre>
 * &#064;Configuration
 * &#064;EnableDataStoreTextWriter
 * static class Config extends SpringDataStoreTextWriterConfigurerAdapter {
 *
 *   &#064;Override
 *   public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
 *     writer
 *       .withNamingStrategy()
 *         .name("foo")
 *         .uuid()
 *         .rolling()
 *         .codec();
 *   }
 *
 * }
 * </pre>
 *
 * @author Janne Valkealahti
 *
 */
public interface NamingStrategyConfigurer extends AnnotationConfigurerBuilder<DataStoreTextWriterConfigurer> {

	/**
	 * Adds a static naming part to a strategy.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .withNamingStrategy()
	 *       .name("name", "prefix");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param name the base name
	 * @param prefix the naming prefix
	 *
	 * @return {@link NamingStrategyConfigurer} for chaining
	 */
	NamingStrategyConfigurer name(String name, String prefix);

	/**
	 * Adds a static naming part to a strategy.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .withNamingStrategy()
	 *       .name("name");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param name the base name
	 *
	 * @return {@link NamingStrategyConfigurer} for chaining
	 */
	NamingStrategyConfigurer name(String name);

	/**
	 * Adds a rolling naming part to a strategy.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .withNamingStrategy()
	 *       .rolling();
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @return {@link NamingStrategyConfigurer} for chaining
	 */
	NamingStrategyConfigurer rolling();

	/**
	 * Adds a codec naming part to a strategy.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .withNamingStrategy()
	 *       .codec();
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @return {@link NamingStrategyConfigurer} for chaining
	 */
	NamingStrategyConfigurer codec();

	/**
	 * Adds an UUID naming part to a strategy.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .withNamingStrategy()
	 *       .uuid();
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @return {@link NamingStrategyConfigurer} for chaining
	 */
	NamingStrategyConfigurer uuid();

}
