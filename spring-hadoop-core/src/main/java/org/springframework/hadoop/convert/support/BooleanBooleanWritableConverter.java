package org.springframework.hadoop.convert.support;

import org.apache.hadoop.io.BooleanWritable;
import org.springframework.core.convert.converter.Converter;

/**
 * @author Bertrand Dechoux
 * 
 */
public class BooleanBooleanWritableConverter implements
        Converter<Boolean, BooleanWritable> {
    public BooleanWritable convert(Boolean source) {
        return new BooleanWritable(source);
    }
}