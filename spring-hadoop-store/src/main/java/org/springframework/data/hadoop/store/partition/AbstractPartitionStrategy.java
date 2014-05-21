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
package org.springframework.data.hadoop.store.partition;

/**
 * Base implementation of {@link PartitionStrategy}.
 *
 * @param <T> the type of an entity to write
 * @param <K> the type of a partition key
 */
public abstract class AbstractPartitionStrategy<T, K> implements PartitionStrategy<T, K> {

	private final PartitionResolver<K> partitionResolver;

	private final PartitionKeyResolver<T, K> partitionKeyResolver;

	/**
	 * Instantiates a new abstract partition strategy.
	 *
	 * @param partitionResolver the partition resolver
	 * @param partitionKeyResolver the partition key resolver
	 */
	public AbstractPartitionStrategy(PartitionResolver<K> partitionResolver,
			PartitionKeyResolver<T, K> partitionKeyResolver) {
		this.partitionResolver = partitionResolver;
		this.partitionKeyResolver = partitionKeyResolver;
	}

	@Override
	public PartitionResolver<K> getPartitionResolver() {
		return partitionResolver;
	}

	@Override
	public PartitionKeyResolver<T, K> getPartitionKeyResolver() {
		return partitionKeyResolver;
	}

}
