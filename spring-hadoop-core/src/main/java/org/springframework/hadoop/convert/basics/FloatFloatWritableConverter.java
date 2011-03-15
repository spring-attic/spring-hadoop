package org.springframework.hadoop.convert.basics;

import org.apache.hadoop.io.FloatWritable;
import org.springframework.core.convert.converter.Converter;

/**
 * @author Bertrand Dechoux
 * 
 */
public class FloatFloatWritableConverter implements
        Converter<Float, FloatWritable> {
    public FloatWritable convert(Float source) {
        return new FloatWritable(source);
    }
}