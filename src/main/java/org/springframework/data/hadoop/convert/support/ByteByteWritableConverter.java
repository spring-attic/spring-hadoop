package org.springframework.data.hadoop.convert.support;

import org.apache.hadoop.io.ByteWritable;
import org.springframework.core.convert.converter.Converter;

/**
 * @author Bertrand Dechoux
 *
 */
public class ByteByteWritableConverter implements Converter<Byte, ByteWritable> {
	public ByteWritable convert(Byte source) {
		return new ByteWritable(source);
	}
}