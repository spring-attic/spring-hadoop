package org.springframework.data.hadoop.convert.support;

import org.apache.hadoop.io.BooleanWritable;
import org.springframework.core.convert.converter.Converter;

/**
 * @author Bertrand Dechoux
 *
 */
public class BooleanWritableBooleanConverter implements Converter<BooleanWritable, Boolean> {
	public Boolean convert(BooleanWritable source) {
		return source.get();
	}
}