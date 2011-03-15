package org.springframework.hadoop.convert.basics;

import org.apache.hadoop.io.DoubleWritable;
import org.springframework.core.convert.converter.Converter;

/**
 * @author Bertrand Dechoux
 *
 */
public class DoubleWritableDoubleConverter implements Converter<DoubleWritable, Double> {
	public Double convert(DoubleWritable source) {
		return source.get();
	}
}