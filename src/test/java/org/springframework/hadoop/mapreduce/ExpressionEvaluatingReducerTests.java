/*
 * Copyright 2006-2011 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.springframework.hadoop.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.core.convert.support.GenericConversionService;

/**
 * @author Dave Syer
 * 
 */
public class ExpressionEvaluatingReducerTests {

	@Test
	public void testReduceThroughWriter() throws Exception {
		class Target {
			@SuppressWarnings("unused")
			public void handle(String key, Iterable<Integer> values, RecordWriter<String, Integer> writer)
					throws Exception {
				int sum = 0;
				for (int value : values) {
					sum += value;
				}
				writer.write(key, sum);
			}
		}
		testSumTwoIntegers(new Target(), "#target.handle(key, values, writer)");
	}

	@Test
	public void testReduceThroughMapOutput() throws Exception {
		class Target {
			@SuppressWarnings("unused")
			public Map<String, Integer> handle(String key, Iterable<Integer> values) throws Exception {
				int sum = 0;
				for (int value : values) {
					sum += value;
				}
				return Collections.singletonMap(key, sum);
			}
		}
		testSumTwoIntegers(new Target(), "#target.handle(key, values)");
	}

	@Test
	public void testReduceThroughMapOutputWithPassthruKey() throws Exception {
		class Target {
			@SuppressWarnings("unused")
			public int handle(Iterable<Integer> values) throws Exception {
				int sum = 0;
				for (int value : values) {
					sum += value;
				}
				return sum;
			}
		}
		testSumTwoIntegers(new Target(), "#target.handle(values)");
	}

	@Test
	public void testReduceWithCollection() throws Exception {
		class Target {
			@SuppressWarnings("unused")
			public int handle(Collection<Integer> values) throws Exception {
				int sum = 0;
				for (int value : values) {
					sum += value;
				}
				return sum;
			}
		}
		testSumTwoIntegers(new Target(), "#target.handle(values)");
	}

	private void testSumTwoIntegers(Object target, String expression) throws Exception {
		final Map<Writable, Writable> map = new HashMap<Writable, Writable>();
		ExpressionEvaluatingReducer<IntWritable> reducer = new ExpressionEvaluatingReducer<IntWritable>(target,
				expression, conversionService(), Text.class, IntWritable.class, Integer.class);
		Reducer<Writable, Writable, Writable, Writable>.Context context = getContextForWritingToMap(map);
		reducer.reduce(new Text("foo"), Arrays.<Writable> asList(new IntWritable(1), new IntWritable(2)), context);
		assertEquals("{foo=3}", map.toString());
	}

	private Reducer<Writable, Writable, Writable, Writable>.Context getContextForWritingToMap(
			final Map<Writable, Writable> map) throws Exception {
		Reducer<Text, IntWritable, Text, IntWritable> dummy = new Reducer<Text, IntWritable, Text, IntWritable>();
		@SuppressWarnings("rawtypes")
		Context other = dummy.new Context(new Configuration(), new TaskAttemptID(),
				Mockito.mock(RawKeyValueIterator.class), null, null, null, null, null, null, Text.class,
				IntWritable.class) {
			@Override
			public void write(Text key, IntWritable value) throws IOException, InterruptedException {
				map.put(key, value);
			}
		};
		@SuppressWarnings("unchecked")
		Reducer<Writable, Writable, Writable, Writable>.Context context = other;
		return context;
	}

	public ConversionService conversionService() {
		GenericConversionService service = ConversionServiceFactory.createDefaultConversionService();
		service.addConverter(new Converter<Iterable<?>, Collection<?>>() {
			public Collection<?> convert(Iterable<?> source) {
				ArrayList<Object> result = new ArrayList<Object>();
				for (Object item : source) {
					result.add(item);
				}
				return result;
			}
		});
		service.addConverter(new Converter<IntWritable, Integer>() {
			public Integer convert(IntWritable source) {
				return source.get();
			}
		});
		service.addConverter(new Converter<Integer, IntWritable>() {
			public IntWritable convert(Integer source) {
				return new IntWritable(source);
			}
		});
		service.addConverter(new Converter<Text, String>() {
			public String convert(Text source) {
				return source.toString();
			}
		});
		service.addConverter(new Converter<String, Text>() {
			public Text convert(String source) {
				return new Text(source);
			}
		});
		return service;
	}
}
