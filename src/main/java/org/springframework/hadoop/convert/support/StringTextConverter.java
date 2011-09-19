package org.springframework.hadoop.convert.support;

import org.apache.hadoop.io.Text;
import org.springframework.core.convert.converter.Converter;

/**
 * @author Dave Syer
 *
 */
public class StringTextConverter implements Converter<String, Text> {
	public Text convert(String source) {
		return new Text(source);
	}
}