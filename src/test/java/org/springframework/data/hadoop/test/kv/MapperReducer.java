/*
 * Copyright 2006-2011 the original author or authors.
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
package org.springframework.data.hadoop.test.kv;

import java.util.List;
import java.util.Map;

import org.springframework.data.hadoop.annotation.Mapper;
import org.springframework.data.hadoop.annotation.Reducer;

/**
 * @author Dave Syer
 *
 */
public class MapperReducer {

	@Mapper
	public void reverse(String key, String value, Map<String, String> output) {
		output.put(value, key);
	}
	
	@Reducer
	public void count(String key, List<String> values, Map<String, Integer> output) {
		output.put(key, values.size());
	}
	
}
