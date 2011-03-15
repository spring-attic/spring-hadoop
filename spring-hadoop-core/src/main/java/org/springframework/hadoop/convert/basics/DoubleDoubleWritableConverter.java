package org.springframework.hadoop.convert.basics;

import org.apache.hadoop.io.DoubleWritable;
import org.springframework.core.convert.converter.Converter;

/**
 * @author Bertrand Dechoux
 * 
 */
public class DoubleDoubleWritableConverter implements
        Converter<Double, DoubleWritable> {
    public DoubleWritable convert(Double source) {
        return new DoubleWritable(source);
    }
}