/*
 * Copyright 2011-2015 the original author or authors.
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
package org.springframework.data.hadoop.hive;

/**
 * Callback interface for Hive code. To be used with {@link HiveTemplate} execute method, often as anonymous
 * classes within a method implementation.
 * 
 * @author Costin Leau
 * @author Thomas Risberg
 */
public interface HiveClientCallback<T> {

	/**
	 * Gets called by {@link HiveTemplate#execute(HiveClientCallback)} with an active {@link HiveClient}. Does not need to
	 * care about activating or closing the {@link HiveClient}, or handling exceptions.
	 * 
	 * Due to the big number of exceptions thrown by {@link HiveClient} (in particular
	 * {@link org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Client}) which do not share any common base class,
	 * the callback signature uses a generic declaration.
	 * For user specific error, consider runtime exceptions which are not translated.
	 * 
	 * @param hiveClient active hive connection
	 * @return action result
	 * @throws Exception if thrown by {@link HiveClient}
	 */
	T doInHive(HiveClient hiveClient) throws Exception;
}
