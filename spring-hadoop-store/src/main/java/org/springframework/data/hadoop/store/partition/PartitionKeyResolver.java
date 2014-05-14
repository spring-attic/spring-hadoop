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
 * A {@code PartitionKeyResolver} is a strategy interface defining
 * entity is automatically translated into a matching partition key.
 *
 * @author Janne Valkealahti
 *
 * @param <T> the type of an entity to write
 * @param <K> the type of a partition key
 */
public interface PartitionKeyResolver<T, K> {

	/**
	 * Resolve partition key for a given entity. This method may return
	 * <code>NULL</code> indicating that there are no partition
	 * key available.
	 *
	 * @param entity the entity
	 * @return the partition key
	 */
	K resolvePartitionKey(T entity);

}
