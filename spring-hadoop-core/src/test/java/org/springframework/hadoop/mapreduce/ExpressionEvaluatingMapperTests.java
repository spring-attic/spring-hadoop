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
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Test;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.core.convert.support.GenericConversionService;

/**
 * @author Dave Syer
 * 
 */
public class ExpressionEvaluatingMapperTests {

	@Test
	public void testMapperThroughWriter() throws Exception {
		class Target {
			@SuppressWarnings("unused")
			public void handle(int key, String value, RecordWriter<String, Integer> writer) throws Exception {
				for (String word : value.split("\\W")) {
					writer.write(word, 1);
				}
			}
		}
		testSplitTwoWords(new Target(), "#target.handle(key, value, writer)");
	}

	@Test
	public void testMapperThroughMapOutputWithPassthruKey() throws Exception {
		class Target {
			@SuppressWarnings("unused")
			public Map<String, Integer> handle(String value) throws Exception {
				Map<String, Integer> map = new LinkedHashMap<String, Integer>();
				for (String word : value.split("\\W")) {
					map.put(word, 1);
				}
				return map;
			}
		}
		testSplitTwoWords(new Target(), "#target.handle(value)");
	}

	private void testSplitTwoWords(Object target, String expression) throws Exception {
		final Map<Writable, Writable> map = new LinkedHashMap<Writable, Writable>();
		ExpressionEvaluatingMapper mapper = new ExpressionEvaluatingMapper(target, expression, conversionService(), Text.class, IntWritable.class);
		Mapper<Writable, Writable, Writable, Writable>.Context context = getContextForWritingToMap(map);
		mapper.map(new IntWritable(1), new Text("foo bar"), context);
		assertEquals("{foo=1, bar=1}", map.toString());
	}

	private Mapper<Writable, Writable, Writable, Writable>.Context getContextForWritingToMap(
			final Map<Writable, Writable> map) throws Exception {
		Mapper<IntWritable, Text, Text, IntWritable> dummy = new Mapper<IntWritable, Text, Text, IntWritable>();
		@SuppressWarnings("rawtypes")
		Mapper.Context other = dummy.new Context(new Configuration(), new TaskAttemptID(), null, null, null, null, null) {
			@Override
			public void write(Text key, IntWritable value) throws IOException, InterruptedException {
				map.put(key, value);
			}
		};
		@SuppressWarnings("unchecked")
		Mapper<Writable, Writable, Writable, Writable>.Context context = other;
		return context;
	}

	public ConversionService conversionService() {
		GenericConversionService service = ConversionServiceFactory.createDefaultConversionService();
		service.addConverter(new Converter<String, Date>() {
			public Date convert(String source) {
				return new Date(100L);
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
