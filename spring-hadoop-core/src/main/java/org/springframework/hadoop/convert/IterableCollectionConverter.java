package org.springframework.hadoop.convert;

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.core.convert.converter.Converter;

/**
 * @author Dave Syer
 *
 */
public class IterableCollectionConverter implements Converter<Iterable<?>, Collection<?>> {
	public Collection<?> convert(Iterable<?> source) {
		ArrayList<Object> result = new ArrayList<Object>();
		for (Object item : source) {
			result.add(item);
		}
		return result;
	}
}