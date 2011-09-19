package org.springframework.hadoop.convert.support;

import org.apache.hadoop.io.LongWritable;
import org.springframework.core.convert.converter.Converter;

/**
 * @author Bertrand Dechoux
 *
 */
public class LongWritableLongConverter implements Converter<LongWritable, Long> {
	public Long convert(LongWritable source) {
		return source.get();
	}
}