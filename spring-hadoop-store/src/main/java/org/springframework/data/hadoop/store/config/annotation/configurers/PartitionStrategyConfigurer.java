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
import org.springframework.data.hadoop.store.partition.DefaultPartitionStrategy;
import org.springframework.data.hadoop.store.partition.PartitionStrategy;

/**
 * {@link AnnotationConfigurerBuilder} for configuring a partition strategy.
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
 *       .withPartitionStrategy()
 *         .map("myspel");
 *   }
 *
 * }
 * </pre>
 *
 * @author Janne Valkealahti
 *
 */
public interface PartitionStrategyConfigurer extends AnnotationConfigurerBuilder<DataStoreTextWriterConfigurer> {

	/**
	 * Specify a custom {@link PartitionStrategy}.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .withPartitionStrategy()
	 *       .custom(new MyCustomPartitionStrategy());
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param partitionStrategy the partition strategy
	 *
	 * @return {@link PartitionStrategyConfigurer} for chaining
	 */
	PartitionStrategyConfigurer custom(PartitionStrategy<?, ?> partitionStrategy);

	/**
	 * Specify to use {@link DefaultPartitionStrategy} with a given
	 * SpEL expression.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .withPartitionStrategy()
	 *       .map("spelexpression");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param expression the partition expression
	 *
	 * @return {@link PartitionStrategyConfigurer} for chaining
	 */
	PartitionStrategyConfigurer map(String expression);

}
