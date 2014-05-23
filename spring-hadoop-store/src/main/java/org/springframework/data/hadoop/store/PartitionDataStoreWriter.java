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
package org.springframework.data.hadoop.store;

import java.io.IOException;

import org.springframework.data.hadoop.store.partition.PartitionStrategy;

/**
 * A {@code DataStorePartitionWriter} is an extension of {@link DataStoreWriter}
 * adding functionality to write entities using a partition key.
 * A partition key is an object which is used by a {@link PartitionStrategy}
 * to determine a partition path to write to.
 *
 * @author Janne Valkealahti
 * @author Rodrigo Meneses
 *
 * @param <T> the type of an entity to write
 * @param <K> the type of a partition key
 */
public interface PartitionDataStoreWriter<T,K> extends DataStoreWriter<T> {

	/**
	 * Write an entity with an explicit partitioning key.
	 *
	 * @param entity the entity to write
	 * @param partitionKey the partition key
	 * @throws IOException if an I/O error occurs
	 */
	void write(T entity, K partitionKey) throws IOException;

}
