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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Tests for {@link DefaultPartitionStrategyTests}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultPartitionStrategyTests {

	@Test
	public void testDefaults() {
		String expression = "region + '/' + dateFormat('yyyy/MM', timestamp)";
		DefaultPartitionStrategy<String> strategy = new DefaultPartitionStrategy<String>(expression);
		DefaultPartitionKey key = new DefaultPartitionKey();
		key.put("region", "foo");
		Path resolvedPath = strategy.getPartitionResolver().resolvePath(key);
		assertThat(resolvedPath, notNullValue());
		String nowYYYYMM = new SimpleDateFormat("yyyy/MM").format(new Date());
		assertThat(resolvedPath.toString(), is("foo/" + nowYYYYMM));
		Map<String, Object> resolvedPartitionKey = strategy.getPartitionKeyResolver().resolvePartitionKey("jee");
		assertThat(resolvedPartitionKey, notNullValue());
	}

	@Test
	public void testHashList() {
		String expression = "list(region,{{'nordic','fin','swe'},{'britain','eng','sco'}})";
		DefaultPartitionStrategy<String> strategy = new DefaultPartitionStrategy<String>(expression);

		DefaultPartitionKey key1 = new DefaultPartitionKey();
		key1.put("region", "fin");
		Path resolvedPath1 = strategy.getPartitionResolver().resolvePath(key1);
		assertThat(resolvedPath1, notNullValue());
		assertThat(resolvedPath1.toString(), is("nordic_list"));

		DefaultPartitionKey key2 = new DefaultPartitionKey();
		key2.put("region", "eng");
		Path resolvedPath2 = strategy.getPartitionResolver().resolvePath(key2);
		assertThat(resolvedPath2, notNullValue());
		assertThat(resolvedPath2.toString(), is("britain_list"));

		DefaultPartitionKey key3 = new DefaultPartitionKey();
		key3.put("region", "usa");
		Path resolvedPath3 = strategy.getPartitionResolver().resolvePath(key3);
		assertThat(resolvedPath3, notNullValue());
		assertThat(resolvedPath3.toString(), is("list"));

		Map<String, Object> resolvedPartitionKey = strategy.getPartitionKeyResolver().resolvePartitionKey("jee");
		assertThat(resolvedPartitionKey, notNullValue());
	}

	@Test
	public void testHash() {
		String expression = "hash(region,2)";
		DefaultPartitionStrategy<String> strategy = new DefaultPartitionStrategy<String>(expression);

		DefaultPartitionKey key1 = new DefaultPartitionKey();
		key1.put("region", "fin");
		Path resolvedPath1 = strategy.getPartitionResolver().resolvePath(key1);
		assertThat(resolvedPath1, notNullValue());
		assertThat(resolvedPath1.toString(), anyOf(is("0_hash"),is("1_hash")));

		DefaultPartitionKey key2 = new DefaultPartitionKey();
		key2.put("region", "eng");
		Path resolvedPath2 = strategy.getPartitionResolver().resolvePath(key2);
		assertThat(resolvedPath2, notNullValue());
		assertThat(resolvedPath2.toString(), anyOf(is("0_hash"),is("1_hash")));

		DefaultPartitionKey key3 = new DefaultPartitionKey();
		key3.put("region", "usa");
		Path resolvedPath3 = strategy.getPartitionResolver().resolvePath(key3);
		assertThat(resolvedPath3, notNullValue());
		assertThat(resolvedPath3.toString(), anyOf(is("0_hash"),is("1_hash")));

		Map<String, Object> resolvedPartitionKey = strategy.getPartitionKeyResolver().resolvePartitionKey("jee");
		assertThat(resolvedPartitionKey, notNullValue());
	}

	@Test
	public void testHashRange() {
		String expression = "range(region,{10,20,30,40})";
		DefaultPartitionStrategy<String> strategy = new DefaultPartitionStrategy<String>(expression);

		DefaultPartitionKey key1 = new DefaultPartitionKey();
		key1.put("region", 15);
		Path resolvedPath1 = strategy.getPartitionResolver().resolvePath(key1);
		assertThat(resolvedPath1, notNullValue());
		assertThat(resolvedPath1.toString(), is("20_range"));

		DefaultPartitionKey key2 = new DefaultPartitionKey();
		key2.put("region", 25);
		Path resolvedPath2 = strategy.getPartitionResolver().resolvePath(key2);
		assertThat(resolvedPath2, notNullValue());
		assertThat(resolvedPath2.toString(), is("30_range"));

		DefaultPartitionKey key3 = new DefaultPartitionKey();
		key3.put("region", 35);
		Path resolvedPath3 = strategy.getPartitionResolver().resolvePath(key3);
		assertThat(resolvedPath3, notNullValue());
		assertThat(resolvedPath3.toString(), is("40_range"));

		DefaultPartitionKey key4 = new DefaultPartitionKey();
		key4.put("region", 40);
		Path resolvedPath4 = strategy.getPartitionResolver().resolvePath(key4);
		assertThat(resolvedPath4, notNullValue());
		assertThat(resolvedPath4.toString(), is("40_range"));

		DefaultPartitionKey key5 = new DefaultPartitionKey();
		key5.put("region", 45);
		Path resolvedPath5 = strategy.getPartitionResolver().resolvePath(key5);
		assertThat(resolvedPath5, notNullValue());
		assertThat(resolvedPath5.toString(), is("40_range"));

		Map<String, Object> resolvedPartitionKey = strategy.getPartitionKeyResolver().resolvePartitionKey("jee");
		assertThat(resolvedPartitionKey, notNullValue());
	}

}
