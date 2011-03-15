package org.springframework.hadoop.convert.basics;

import org.apache.hadoop.io.Text;
import org.springframework.core.convert.converter.Converter;

/**
 * @author Dave Syer
 *
 */
public class TextStringConverter implements Converter<Text, String> {
	public String convert(Text source) {
		return source.toString();
	}
}