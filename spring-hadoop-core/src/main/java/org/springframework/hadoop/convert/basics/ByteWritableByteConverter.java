package org.springframework.hadoop.convert.basics;

import org.apache.hadoop.io.ByteWritable;
import org.springframework.core.convert.converter.Converter;

/**
 * @author Bertrand Dechoux
 *
 */
public class ByteWritableByteConverter implements Converter<ByteWritable, Byte> {
	public Byte convert(ByteWritable source) {
		return source.get();
	}
}