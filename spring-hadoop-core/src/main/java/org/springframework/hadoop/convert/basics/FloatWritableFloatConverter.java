package org.springframework.hadoop.convert.basics;

import org.apache.hadoop.io.FloatWritable;
import org.springframework.core.convert.converter.Converter;

/**
 * @author Bertrand Dechoux
 *
 */
public class FloatWritableFloatConverter implements Converter<FloatWritable, Float> {
	public Float convert(FloatWritable source) {
		return source.get();
	}
}