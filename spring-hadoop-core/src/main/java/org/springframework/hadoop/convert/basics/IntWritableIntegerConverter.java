package org.springframework.hadoop.convert.basics;

import org.apache.hadoop.io.IntWritable;
import org.springframework.core.convert.converter.Converter;

/**
 * @author Dave Syer
 *
 */
public class IntWritableIntegerConverter implements Converter<IntWritable, Integer> {
	public Integer convert(IntWritable source) {
		return source.get();
	}
}