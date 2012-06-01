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
package org.springframework.data.hadoop.cascading.tap.local;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.springframework.integration.core.MessageHandler;
import org.springframework.integration.message.GenericMessage;

/**
 * {@link OutputStream} implementation backed by a {@link MessageHandler}.
 * 
 * @author Costin Leau
 */
class MessageHandlerOutputStream extends OutputStream {

	private final MessageHandler handler;
	private List<Byte> buffer = new ArrayList<Byte>();

	MessageHandlerOutputStream(MessageHandler handler) {
		this.handler = handler;
	}

	@Override
	public void write(int b) throws IOException {
		buffer.add(Byte.valueOf((byte) b));
	}

	/**
	 * Hint method used to indicate that a message has finished, meaning the previously
	 * written bytes (if any) can be assembled.
	 */
	void endMessage() {
		if (!buffer.isEmpty()) {
			byte[] raw = new byte[buffer.size()];
			for (int i = 0; i < raw.length; i++) {
				raw[i] = buffer.get(i).byteValue();
			}
			buffer.clear();
			handler.handleMessage(new GenericMessage<byte[]>(raw));
		}
	}

	@Override
	public void flush() throws IOException {
		endMessage();
	}

	@Override
	public void close() throws IOException {
		buffer.clear();
	}

	@Override
	public String toString() {
		return "OutputStream for " + handler;
	}
}
