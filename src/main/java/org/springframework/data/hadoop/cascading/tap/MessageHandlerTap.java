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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.springframework.integration.core.MessageHandler;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkTap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntrySchemeCollector;

/**
 * {@link SinkTap} on top of Spring Integration {@link MessageHandler}.
 * <p/>
 * The creation of the message depends on the behaviour of the underlying stream.
 * Each time it is flushed, a Message will be created (with payload of type byte[]).
 * If supported by the underlying stream, a message will be created for each tuple.
 * 
 * @author Costin Leau
 */
public class MessageHandlerTap extends SinkTap<Properties, OutputStream> {

	private final MessageHandlerOutputStream handler;
	private long modifiedTime = -1;

	public MessageHandlerTap(Scheme<Properties, ?, OutputStream, ?, ?> scheme, MessageHandler handler) {
		super(scheme);
		this.handler = new MessageHandlerOutputStream(handler);
	}

	@Override
	public String getIdentifier() {
		return handler.toString();
	}

	@Override
	public TupleEntryCollector openForWrite(FlowProcess<Properties> flowProcess, OutputStream output)
			throws IOException {
		
		if (output == null) {
			// wrap tuple entry to indicate when an entry has finished
			return new TupleEntrySchemeCollector<Properties, OutputStream>(flowProcess, getScheme(), handler, getIdentifier()) {
				@Override
				protected void collect(TupleEntry tupleEntry) throws IOException {
					super.collect(tupleEntry);
					handler.endMessage();
				}
			};
		}
		return new TupleEntrySchemeCollector<Properties, OutputStream>(flowProcess, getScheme(), output, getIdentifier());
	}

	@Override
	public boolean createResource(Properties conf) throws IOException {
		return true;
	}

	@Override
	public boolean deleteResource(Properties conf) throws IOException {
		return false;
	}

	@Override
	public boolean resourceExists(Properties conf) throws IOException {
		return true;
	}

	@Override
	public long getModifiedTime(Properties conf) throws IOException {
		return modifiedTime;
	}
}