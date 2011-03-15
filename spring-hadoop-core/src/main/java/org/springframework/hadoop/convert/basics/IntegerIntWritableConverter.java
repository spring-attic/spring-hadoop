package org.springframework.hadoop.convert.basics;

import org.apache.hadoop.io.IntWritable;
import org.springframework.core.convert.converter.Converter;

/**
 * @author Dave Syer
 *
 */
public class IntegerIntWritableConverter implements Converter<Integer, IntWritable> {
	public IntWritable convert(Integer source) {
		return new IntWritable(source);
	}
}