/*
 * Copyright 2011-2012 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.cascading.tap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;

import org.junit.Test;
import org.springframework.data.hadoop.TestSinkTap;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.data.hadoop.cascading.tap.local.MessageHandlerTap;
import org.springframework.data.hadoop.cascading.tap.local.MessageSourceTap;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.transformer.FileToByteArrayTransformer;
import org.springframework.integration.stream.ByteStreamReadingMessageSource;
import org.springframework.integration.stream.ByteStreamWritingMessageHandler;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import static org.junit.Assert.assertTrue;

/**
 * @author Costin Leau
 */
public class MessageTapTest {

	{
		TestUtils.hackHadoopStagingOnWin();
	}

	@Test
	public void testMessageSourceTextLineScheme() throws Exception {
		Scheme sourceScheme = new TextLine(new Fields("line"));
		Tap source = new MessageSourceTap(sourceScheme, 
				new ByteStreamReadingMessageSource(getClass().getResourceAsStream("/data/apache-short.txt")));
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Tap sink = new TestSinkTap(sourceScheme, out);

		Pipe pipe = new Pipe("io");

		Flow flow = new LocalFlowConnector().connect(source, sink, pipe);
		flow.complete();

		byte[] byteArray = out.toByteArray();
		InputStream stream = getClass().getResourceAsStream("/data/apache-short.txt");
		assertTrue(TestUtils.compareStreams(stream, new ByteArrayInputStream(byteArray)));
	}
	
	@Test
	public void testMessageSourceTextDelimitedScheme() throws Exception {
		Scheme sourceScheme = new TextDelimited(new Fields("name", "definition"), ",");
		Tap source = new MessageSourceTap(sourceScheme, new ByteStreamReadingMessageSource(
				getClass().getResourceAsStream("/data/babynames-short.txt")));

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Tap sink = new TestSinkTap(sourceScheme, out);

		Pipe pipe = new Pipe("io");

		Flow flow = new LocalFlowConnector().connect(source, sink, pipe);
		flow.complete();
		String str = out.toString();
		assertTrue(str.contains("AARON"));
		assertTrue(str.contains("ABIBA"));
	}

	@Test
	public void testMessageSourceWithTransformer() throws Exception {
		Scheme sourceScheme = new TextLine(new Fields("line"));
		
		FileReadingMessageSource fileSource = new FileReadingMessageSource();
		URI uri = getClass().getResource("/data").toURI();
		fileSource.setDirectory(new File(uri));
		CompositeFileListFilter f = new CompositeFileListFilter();
		f.addFilter(new SimplePatternFileListFilter("apache-short.txt"));
		f.addFilter(new AcceptOnceFileListFilter());
		fileSource.setFilter(f);
		
		Tap source = new MessageSourceTap(sourceScheme, 
				fileSource,
				new FileToByteArrayTransformer());
		

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Tap sink = new TestSinkTap(sourceScheme, out);

		Pipe pipe = new Pipe("io");

		Flow flow = new LocalFlowConnector().connect(source, sink, pipe);
		flow.complete();

		byte[] byteArray = out.toByteArray();
		InputStream stream = getClass().getResourceAsStream("/data/apache-short.txt");
		assertTrue(TestUtils.compareStreams(stream, new ByteArrayInputStream(byteArray)));
	}

	@Test
	public void testMessageHandler() throws Exception {
		Scheme scheme = new TextDelimited(new Fields("name", "definition"), ",");
		Tap source = new MessageSourceTap(scheme, 
				new ByteStreamReadingMessageSource(getClass().getResourceAsStream("/data/babynames-short.txt")));
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Tap sink = new MessageHandlerTap(new TextLine(new Fields("line")), new ByteStreamWritingMessageHandler(out));
		Pipe pipe = new Pipe("io");
		Flow flow = new LocalFlowConnector().connect(source, sink, pipe);
		flow.complete();

		String str = out.toString();
		assertTrue(str.contains("AARON"));
		assertTrue(str.contains("ABIBA"));
	}
}
