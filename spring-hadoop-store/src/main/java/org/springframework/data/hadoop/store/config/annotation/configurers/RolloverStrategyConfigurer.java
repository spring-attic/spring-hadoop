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
 * {@link AnnotationConfigurerBuilder} for configuring a rollover strategy.
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
 *       .withRolloverStrategy()
 *         .size("1M");
 *   }
 *
 * }
 * </pre>
 *
 * @author Janne Valkealahti
 *
 */
public interface RolloverStrategyConfigurer extends AnnotationConfigurerBuilder<DataStoreTextWriterConfigurer> {

	/**
	 * Specify a rollover size in bytes.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .withRolloverStrategy()
	 *       .size(10000);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param size the size
	 *
	 * @return {@link RolloverStrategyConfigurer} for chaining
	 */
	RolloverStrategyConfigurer size(long size);

	/**
	 * Specify a rollover size. Supported string representations of a size
	 * are, 1K, 1M, 1G and 1T where numeric part can be changed.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .withRolloverStrategy()
	 *       .size("1M");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param size the size
	 *
	 * @return {@link RolloverStrategyConfigurer} for chaining
	 */
	RolloverStrategyConfigurer size(String size);

}
