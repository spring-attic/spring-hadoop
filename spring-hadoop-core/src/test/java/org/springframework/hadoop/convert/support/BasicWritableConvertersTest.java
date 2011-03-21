package org.springframework.hadoop.convert.support;

import junit.framework.Assert;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.hadoop.convert.support.BooleanBooleanWritableConverter;
import org.springframework.hadoop.convert.support.BooleanWritableBooleanConverter;
import org.springframework.hadoop.convert.support.ByteByteWritableConverter;
import org.springframework.hadoop.convert.support.ByteWritableByteConverter;
import org.springframework.hadoop.convert.support.DoubleDoubleWritableConverter;
import org.springframework.hadoop.convert.support.DoubleWritableDoubleConverter;
import org.springframework.hadoop.convert.support.FloatFloatWritableConverter;
import org.springframework.hadoop.convert.support.FloatWritableFloatConverter;
import org.springframework.hadoop.convert.support.IntWritableIntegerConverter;
import org.springframework.hadoop.convert.support.IntegerIntWritableConverter;
import org.springframework.hadoop.convert.support.LongLongWritableConverter;
import org.springframework.hadoop.convert.support.LongWritableLongConverter;
import org.springframework.hadoop.convert.support.StringTextConverter;
import org.springframework.hadoop.convert.support.TextStringConverter;


/**
 * Test that conversion of primitive types (or their Wrappers) is handled.
 * {@link String} is included.
 */
public class BasicWritableConvertersTest {
    
    @Test
    public void shouldHandleBoolean() {
        BooleanBooleanWritableConverter toWritableConverter = new BooleanBooleanWritableConverter();
        BooleanWritableBooleanConverter fromWritableConverter = new BooleanWritableBooleanConverter();
        
        assertBackAndForthConversion(new Boolean(true), toWritableConverter, fromWritableConverter);
        assertBackAndForthConversion(new BooleanWritable(true), fromWritableConverter, toWritableConverter);
    }
    
    @Test
    public void shouldHandleByte() {
        ByteByteWritableConverter toWritableConverter = new ByteByteWritableConverter();
        ByteWritableByteConverter fromWritableConverter = new ByteWritableByteConverter();
        
        assertBackAndForthConversion(new Byte((byte)101), toWritableConverter, fromWritableConverter);
        assertBackAndForthConversion(new ByteWritable((byte) 101), fromWritableConverter, toWritableConverter);
    }
    
    @Test
    public void shouldHandleInteger() {
        IntegerIntWritableConverter toWritableConverter = new IntegerIntWritableConverter();
        IntWritableIntegerConverter fromWritableConverter = new IntWritableIntegerConverter();
        
        assertBackAndForthConversion(new Integer(42), toWritableConverter, fromWritableConverter);
        assertBackAndForthConversion(new IntWritable(42), fromWritableConverter, toWritableConverter);
    }
    
    @Test
    public void shouldHandleLong() {
        LongLongWritableConverter toWritableConverter = new LongLongWritableConverter();
        LongWritableLongConverter fromWritableConverter = new LongWritableLongConverter();
        
        assertBackAndForthConversion(new Long(42000L), toWritableConverter, fromWritableConverter);
        assertBackAndForthConversion(new LongWritable(42000L), fromWritableConverter, toWritableConverter);
    }
    
    @Test
    public void shouldHandleFloat() {
        FloatFloatWritableConverter toWritableConverter = new FloatFloatWritableConverter();
        FloatWritableFloatConverter fromWritableConverter = new FloatWritableFloatConverter();
        
        assertBackAndForthConversion(new Float(5.2f), toWritableConverter, fromWritableConverter);
        assertBackAndForthConversion(new FloatWritable(5.2f), fromWritableConverter, toWritableConverter);
    }
    
    @Test
    public void shouldHandleDouble() {
        DoubleDoubleWritableConverter toWritableConverter = new DoubleDoubleWritableConverter();
        DoubleWritableDoubleConverter fromWritableConverter = new DoubleWritableDoubleConverter();
        
        assertBackAndForthConversion(new Double(5.2), toWritableConverter, fromWritableConverter);
        assertBackAndForthConversion(new DoubleWritable(5.2), fromWritableConverter, toWritableConverter);
    }
    
    @Test
    public void shouldHandleString() {
        StringTextConverter toWritableConverter = new StringTextConverter();
        TextStringConverter fromWritableConverter = new TextStringConverter();
        
        assertBackAndForthConversion("string", toWritableConverter, fromWritableConverter);
        assertBackAndForthConversion(new Text("text"), fromWritableConverter, toWritableConverter);
    }
   
    private <S,T> void assertBackAndForthConversion(final T value, final Converter<T, S> firstConverter, final Converter<S,T> secondConverter) {
        Assert.assertEquals(value, secondConverter.convert(firstConverter.convert(value)));
    }

}
