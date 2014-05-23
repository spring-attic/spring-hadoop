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

import java.util.HashMap;
import java.util.Map;

/**
 * A default partition key which simply is a {@link Map}. This
 * key can be used together with {@link DefaultPartitionStrategy}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultPartitionKey extends HashMap<String, Object> {

	private static final long serialVersionUID = 6493472568899727029L;

	public static final String KEY_TIMESTAMP = "timestamp";

	public static final String KEY_CONTENT = "content";

	/**
	 * Instantiates a new default partition key.
	 */
	public DefaultPartitionKey() {
		super();
		put(KEY_TIMESTAMP, System.currentTimeMillis());
	}

	/**
	 * Instantiates a new default partition key using
	 * a given timestamp.
	 *
	 * @param timestamp the timestamp
	 */
	public DefaultPartitionKey(long timestamp) {
		super();
		put(KEY_TIMESTAMP, timestamp);
	}

	/**
	 * Instantiates a new default partition key using
	 * a given timestamp and a content object.
	 *
	 * @param timestamp the timestamp
	 * @param content the content
	 */
	public DefaultPartitionKey(long timestamp, Object content) {
		super();
		put(KEY_TIMESTAMP, timestamp);
		put(KEY_CONTENT, content);
	}

	/**
	 * Instantiates a new default partition key using
	 * a content object.
	 *
	 * @param content the content
	 */
	public DefaultPartitionKey(Object content) {
		super();
		put(KEY_TIMESTAMP, System.currentTimeMillis());
		put(KEY_CONTENT, content);
	}

	/**
	 * Instantiates a new default partition key
	 * with a {@link Map}.
	 *
	 * @param map the instance of map
	 */
	public DefaultPartitionKey(Map<? extends String, ? extends Object> map) {
		super(map);
	}

}
