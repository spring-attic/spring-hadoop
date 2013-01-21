/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.hadoop.cascading.tap.local;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.springframework.integration.core.MessageSource;
import org.springframework.integration.transformer.Transformer;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SourceTap;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIterator;

/**
 * {@link SourceTap} for Spring Integration {@link MessageSource}.
 * 
 * @author Costin Leau
 */
public class MessageSourceTap extends SourceTap<Properties, InputStream> {

	private InputStream source;

	/**
	 * Constructs a new <code>MessageSourceTap</code> instance.
	 *
	 * @param scheme data scheme
	 * @param source a byte[] based message source
	 */
	public MessageSourceTap(Scheme<Properties, InputStream, ?, ?, ?> scheme, MessageSource<byte[]> source) {
		super(scheme);
		this.source = new MessageSourceInputStream(source);
	}

	/**
	 * Constructs a new <code>MessageSourceTap</code> instance. Allows arbitrary message sources to be passed in along-side
	 * a transformer acting as a serializer.
	 *
	 * @param scheme data scheme
	 * @param source an arbitrary message source
	 * @param transformer a transformer converting the given message source type to byte[] 
	 */
	public MessageSourceTap(Scheme<Properties, InputStream, ?, ?, ?> scheme, MessageSource<?> source, Transformer transformer) {
		this(scheme, new TransformingMessageSource<byte[]>(source, transformer));
	}

	@Override
	public String getIdentifier() {
		return source.toString();
	}

	@Override
	public TupleEntryIterator openForRead(FlowProcess<Properties> flowProcess, InputStream input) throws IOException {
		InputStream in = (input == null ? source : input);
		return new TupleEntrySchemeIterator<Properties, InputStream>(flowProcess, getScheme(), in, getIdentifier());
	}

	@Override
	public boolean resourceExists(Properties conf) throws IOException {
		return true;
	}

	@Override
	public long getModifiedTime(Properties conf) throws IOException {
		return 0;
	}
}