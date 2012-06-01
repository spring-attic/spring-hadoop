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
import java.io.InputStream;

import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.data.hadoop.TestSinkTap;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.data.hadoop.cascading.tap.local.ResourceTap;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import static org.junit.Assert.*;

/**
 * @author Costin Leau
 */
public class ResourceTapTest {

	{
		TestUtils.hackHadoopStagingOnWin();
	}

	private ResourceLoader loader = new DefaultResourceLoader(getClass().getClassLoader());

	@Test
	public void testTextLineScheme() throws Exception {
		Resource resource = loader.getResource("/data/apache-short.txt");
		assertTrue(resource.exists());

		ByteArrayOutputStream out = new ByteArrayOutputStream((int) resource.contentLength());

		Scheme sourceScheme = new TextLine(new Fields("line"));
		Tap source = new ResourceTap(sourceScheme, resource);
		Tap sink = new TestSinkTap(sourceScheme, out);

		Pipe pipe = new Pipe("io");

		Flow flow = new LocalFlowConnector().connect(source, sink, pipe);
		flow.complete();

		byte[] byteArray = out.toByteArray();
		InputStream stream = resource.getInputStream();
		assertTrue(TestUtils.compareStreams(stream, new ByteArrayInputStream(byteArray)));
	}

	@Test
	public void testTextDelimited() throws Exception {
		Resource resource = loader.getResource("/data/babynames-short.txt");
		assertTrue(resource.exists());
		ByteArrayOutputStream out = new ByteArrayOutputStream((int) resource.contentLength());

		Scheme sourceScheme = new TextDelimited(new Fields("name", "definition"), ",");
		Tap source = new ResourceTap(sourceScheme, resource);
		Tap sink = new TestSinkTap(new TextLine(new Fields("line")), out);
		Pipe pipe = new Pipe("io");
		Flow flow = new LocalFlowConnector().connect(source, sink, pipe);
		flow.complete();
		String str = out.toString();
		assertTrue(str.contains("AARON"));
		assertTrue(str.contains("ABIBA"));
	}

	@Test
	public void testMultipleResources() throws Exception {
		Resource[] resources = new PathMatchingResourcePatternResolver(loader).getResources("/data/*.txt");
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		Scheme sourceScheme = new TextLine(new Fields("line"));
		Tap source = new ResourceTap(sourceScheme, resources);

		Tap sink = new TestSinkTap(sourceScheme, out);
		Pipe pipe = new Pipe("io");
		Flow flow = new LocalFlowConnector().connect(source, sink, pipe);
		flow.complete();
		String str = out.toString();

		assertTrue(str.contains("Mozilla"));
		assertTrue(str.contains("AARON"));
		assertTrue(str.contains("ABIBA"));
	}
}