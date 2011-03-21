package org.springframework.hadoop.convert.support;

import org.apache.hadoop.io.LongWritable;
import org.springframework.core.convert.converter.Converter;

/**
 * @author Bertrand Dechoux
 *
 */
public class LongLongWritableConverter implements Converter<Long, LongWritable> {
	public LongWritable convert(Long source) {
		return new LongWritable(source);
	}
}