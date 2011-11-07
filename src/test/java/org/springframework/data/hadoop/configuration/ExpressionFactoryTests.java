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
package org.springframework.data.hadoop.configuration;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.junit.Test;
import org.springframework.data.hadoop.annotation.Key;
import org.springframework.data.hadoop.annotation.Mapper;
import org.springframework.data.hadoop.annotation.Reducer;
import org.springframework.data.hadoop.annotation.Value;
import org.springframework.data.hadoop.mapreduce.ExpressionFactory;

/**
 * @author Dave Syer
 *
 */
public class ExpressionFactoryTests {
	
	private ExpressionFactory factory = new ExpressionFactory();

	@Test
	public void testSimpleMapper() {
		class Target {
			@SuppressWarnings("unused")
			public void map(Long key, String value) {
			}
		}
		assertEquals("#target.map(key,value)", factory.getMapperExpression(new Target()));
	}

	@Test
	public void testMapperWithMap() {
		class Target {
			@SuppressWarnings("unused")
			public void map(Long key, String value, Map<String, Integer> output) {
			}
		}
		assertEquals("#target.map(key,value,map)", factory.getMapperExpression(new Target()));
	}

	@Test
	public void testMapperWithWriter() {
		class Target {
			@SuppressWarnings("unused")
			public void map(Long key, String value, RecordWriter<String, Integer> output) {
			}
		}
		assertEquals("#target.map(key,value,writer)", factory.getMapperExpression(new Target()));
	}

	@Test
	public void testMapperWithContext() {
		class Target {
			@SuppressWarnings("unused")
			public void map(Long key, String value, TaskInputOutputContext<?,?,?,?> output) {
			}
		}
		assertEquals("#target.map(key,value,context)", factory.getMapperExpression(new Target()));
	}

	@Test
	public void testAnnotatedMapper() {
		class Target {
			@SuppressWarnings("unused")
			@Mapper
			public void map(Long key, String value, Map<String, Integer> output) {
			}
			@SuppressWarnings("unused")
			@Reducer
			public void reduce(Collection<String> values, Map<String, Integer> output) {
			}
		}
		assertEquals("#target.map(key,value,map)", factory.getMapperExpression(new Target()));
	}

	@Test
	public void testMapperWithNoKey() {
		class Target {
			@SuppressWarnings("unused")
			public void map(String value, Map<String, Integer> output) {
			}
		}
		assertEquals("#target.map(value,map)", factory.getMapperExpression(new Target()));
	}

	@Test
	public void testMapperWithAnnotations() {
		class Target {
			@SuppressWarnings("unused")
			public void map(@Value String value, @Key Long key, Map<String, Integer> output) {
			}
		}
		assertEquals("#target.map(value,key,map)", factory.getMapperExpression(new Target()));
	}

	@Test
	public void testSimpleReducer() {
		class Target {
			@SuppressWarnings("unused")
			public void handle(Long key, Collection<String> values) {
			}
		}
		assertEquals("#target.handle(key,values)", factory.getReducerExpression(new Target()));
	}

	@Test
	public void testReducerIterableReturningOutput() {
		class Target {
			@SuppressWarnings("unused")
			public int handle(Iterable<String> values) {
				return 0;
			}
		}
		assertEquals("#target.handle(values)", factory.getReducerExpression(new Target()));
	}

	@Test
	public void testReducerCollectionReturningOutput() {
		class Target {
			@SuppressWarnings("unused")
			public int handle(Collection<String> values) {
				return 0;
			}
		}
		assertEquals("#target.handle(values)", factory.getReducerExpression(new Target()));
	}

}
