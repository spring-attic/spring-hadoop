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
package org.springframework.data.hadoop.configuration;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.hadoop.convert.IterableToIterableConverter;
import org.springframework.data.hadoop.mapreduce.ConversionServiceRecordWriter;
import org.springframework.data.hadoop.util.ConversionServiceIterableAdapter;
import org.springframework.expression.spel.SpelEvaluationException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.expression.spel.support.StandardTypeConverter;

/**
 * @author Dave Syer
 * 
 */
public class ConversionTests {

	private Date date;

	private SpelExpressionParser parser;

	private StandardEvaluationContext context;

	private Collection<String> things;

	public void setDate(Date date) {
		this.date = date;
	}

	public void setCollection(Collection<String> things) {
		this.things = things;
	}

	public void setIterable(Iterable<String> things) {
		this.things = new ArrayList<String>();
		for (String thing : things) {
			this.things.add(thing);
		}
	}

	private TaskInputOutputContext<Writable, Writable, ? super Writable, ? super Writable> writer;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Before
	public void init() throws Exception {
		context = new StandardEvaluationContext();
		context.setTypeConverter(new StandardTypeConverter(conversionService()));
		parser = new SpelExpressionParser();
		Reducer reducer = new Reducer();
		writer = reducer.new Context(new Configuration(), new TaskAttemptID(), Mockito.mock(RawKeyValueIterator.class),
				null, null, null, null, null, null, Text.class, IntWritable.class) {

			private Map<Object, Object> map = new HashMap<Object, Object>();

			@Override
			public void write(Object key, Object value) throws IOException, InterruptedException {
				map.put(key, value);
			}

			@Override
			public String toString() {
				return map.toString();
			}

		};
	}

	@Test
	public void testSimpleConversion() throws Exception {
		parser.parseExpression("setDate('foo')").getValue(context, this);
		assertEquals(new Date(100L), date);
	}

	@Test
	public void testCollectionConversion() throws Exception {
		context.setVariable("things", Arrays.asList(new Text("foo"), new Text("bar")));
		parser.parseExpression("collection=#things").getValue(context, this);
		assertEquals("[foo, bar]", things.toString());
	}

	// This doesn't work because TypeDescriptor has Collection as a special case
	// but not Iterable
	@Test(expected = SpelEvaluationException.class)
	public void testIterableConversion() throws Exception {
		context.setVariable("things", Arrays.asList(new Text("foo"), new Text("bar")));
		parser.parseExpression("iterable=#things").getValue(context, this);
		assertEquals("[foo, bar]", things.toString());
	}

	@Test
	public void testMapper() throws Exception {
		context.setVariable("value", new Text("foo bar"));
		context.setVariable("writer", new ConversionServiceRecordWriter(this.writer, conversionService(), Text.class,
				IntWritable.class));
		parser.parseExpression("map(#value, #writer)").getValue(context, new PojoMapper());
		assertEquals("{foo=1, bar=1}", this.writer.toString());
	}

	@Test
	public void testReducerWithIntegers() throws Exception {
		context.setVariable("key", new Text("foo"));
		context.setVariable("values", Arrays.asList(1, 2, 3));
		context.setVariable("writer", new ConversionServiceRecordWriter(this.writer, conversionService(), Text.class,
				IntWritable.class));
		parser.parseExpression("reduce(#key, #values, #writer)").getValue(context, new PojoReducer());
		assertEquals("{foo=6}", this.writer.toString());
	}

	@Test
	public void testReducer() throws Exception {
		context.setVariable("key", new Text("foo"));
		context.setVariable(
				"values",
				new ConversionServiceIterableAdapter<IntWritable, Integer>(Arrays.asList(new IntWritable(1),
						new IntWritable(2), new IntWritable(3)), Integer.class, conversionService()));
		context.setVariable("writer", new ConversionServiceRecordWriter(this.writer, conversionService(), Text.class,
				IntWritable.class));
		parser.parseExpression("reduce(#key, #values, #writer)").getValue(context, new PojoReducer());
		assertEquals("{foo=6}", this.writer.toString());
	}

	public ConversionService conversionService() {
		GenericConversionService service = ConversionServiceFactory.createDefaultConversionService();
		service.addConverter(new Converter<String, Date>() {
			public Date convert(String source) {
				return new Date(100L);
			}
		});
		service.addConverter(new IterableToIterableConverter(service));
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

	public class PojoReducer {
		public void reduce(Writable key, Iterable<Integer> values, RecordWriter<Writable, Integer> writer)
				throws InterruptedException, IOException {
			int sum = 0;
			for (Integer val : values) {
				sum += val;
			}
			writer.write(key, sum);
		}
	}

	public static class PojoMapper {
		public void map(String value, RecordWriter<String, Integer> writer) throws InterruptedException, IOException {
			StringTokenizer itr = new StringTokenizer(value);
			while (itr.hasMoreTokens()) {
				writer.write(itr.nextToken(), 1);
			}
		}
	}

}
