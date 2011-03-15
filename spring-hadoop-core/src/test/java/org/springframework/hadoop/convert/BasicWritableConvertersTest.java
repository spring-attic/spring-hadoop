package org.springframework.hadoop.convert;

import junit.framework.Assert;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.springframework.core.convert.converter.Converter;


/**
 * Test that conversion of primitive types (or their Wrappers) is handled.
 * {@link String} is included.
 */
public class BasicWritableConvertersTest {
    
    @Test
    public void shouldHandleBoolean() {
        Assert.fail("Not yet implemented.");
    }
    
    @Test
    public void shouldHandleByte() {
        Assert.fail("Not yet implemented.");
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
        Assert.fail("Not yet implemented.");
    }
    
    @Test
    public void shouldHandleFloat() {
        Assert.fail("Not yet implemented.");
    }
    
    @Test
    public void shouldHandleDouble() {
        Assert.fail("Not yet implemented.");
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
