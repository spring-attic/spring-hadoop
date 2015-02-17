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

import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurer;
import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurerAdapter;
import org.springframework.data.hadoop.store.config.annotation.SpringDataStoreWriterConfigs;
import org.springframework.data.hadoop.store.config.annotation.builders.DataStoreTextWriterBuilder;
import org.springframework.data.hadoop.store.config.annotation.builders.DataStoreTextWriterConfigurer;
import org.springframework.data.hadoop.store.partition.DefaultPartitionStrategy;
import org.springframework.data.hadoop.store.partition.PartitionStrategy;

/**
 * {@link AnnotationConfigurer} which knows howto configure a partition strategy.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultPartitionStrategyConfigurer extends
		AnnotationConfigurerAdapter<SpringDataStoreWriterConfigs, DataStoreTextWriterConfigurer, DataStoreTextWriterBuilder>
		implements PartitionStrategyConfigurer {

	private PartitionStrategy<?, ?> partitionStrategy;

	@Override
	public void configure(DataStoreTextWriterBuilder builder) throws Exception {
		if (partitionStrategy != null) {
			builder.setPartitionStrategy(partitionStrategy);
		}
	}

	@Override
	public PartitionStrategyConfigurer custom(PartitionStrategy<?, ?> partitionStrategy) {
		this.partitionStrategy = partitionStrategy;
		return this;
	}

	@Override
	public PartitionStrategyConfigurer map(String expression) {
		this.partitionStrategy = new DefaultPartitionStrategy<Object>(expression);
		return this;
	}

}
