/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.yarn.integration.ip.mind;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.serializer.Deserializer;
import org.springframework.core.serializer.Serializer;
import org.springframework.integration.ip.tcp.serializer.SoftEndOfStreamException;

/**
 * Spring {@link Serializer} and {@link Deserializer} interfaces
 * for mind protocol.
 *
 * @author Janne Valkealahti
 *
 */
public class MindRpcSerializer implements Serializer<MindRpcMessageHolder>, Deserializer<MindRpcMessageHolder> {

	private final static Log log = LogFactory.getLog(MindRpcSerializer.class);

	protected int maxMessageSize = 20000;

	/**
	 * @see org.springframework.core.serializer.Deserializer#deserialize(java.io.InputStream)
	 */
	@Override
	public MindRpcMessageHolder deserialize(InputStream inputStream) throws IOException {
		int lenghts[] = readHeader(inputStream);
		if(log.isDebugEnabled()) {
			log.debug("rpc lenghts: " + lenghts[0] + "/" + lenghts[1]);
		}
		Map<String, String> headers = readHeaders(inputStream, lenghts[0]);
		byte[] content = readBytes(inputStream, lenghts[1]);
		if(log.isDebugEnabled()) {
			log.debug("deserialize: " + content);
		}
		return new MindRpcMessageHolder(headers, content);
	}

	/**
	 * @see org.springframework.core.serializer.Serializer#serialize(java.lang.Object, java.io.OutputStream)
	 */
	@Override
	public void serialize(MindRpcMessageHolder object, OutputStream outputStream) throws IOException {
		if(log.isDebugEnabled()) {
			log.debug("serialize length=" + object.toBytes().length + " :"  + new String(object.toBytes()));
		}
		outputStream.write(object.toBytes());
		outputStream.flush();
	}

	/**
	 * Sets the max message size for transport.
	 *
	 * @param maxMessageSize the length of max message
	 */
	public void setMaxMessageSize(int maxMessageSize) {
		this.maxMessageSize = maxMessageSize;
	}

	/**
	 * Reads the first line as a protocol header and parses sizes of
	 * underlying headers and content.
	 *
	 * @param inputStream the input stream
	 * @return int array containing sizes of headers and content
	 * @throws IOException if read error occured
	 */
	protected int[] readHeader(InputStream inputStream) throws IOException {
		int[] ret = new int[]{0,0};
		byte[] buffer = new byte[20];
		int n = 0;
		int bite;
		while (true) {
			bite = inputStream.read();
			if (bite < 0 && n == 0) {
				throw new SoftEndOfStreamException("Stream closed between payloads");
			}
			checkClosure(bite);
			if (n > 0 && bite == '\n' && buffer[n-1] == '\r') {
				break;
			}
			buffer[n++] = (byte) bite;
			if (n >= 20) {
				throw new IOException("CRLF not found before max message length: "
						+ this.maxMessageSize);
			}

		}
		byte[] assembledData = new byte[n-1];
		System.arraycopy(buffer, 0, assembledData, 0, n-1);
		String header = new String(assembledData);

		if(log.isDebugEnabled()) {
			log.debug("Mind rpc header:" + header);
		}

		String[] respBytes = header.split(" ");
		ret[0] = Integer.parseInt(respBytes[1]);
		ret[1] = Integer.parseInt(respBytes[2]);

		if(log.isDebugEnabled()) {
			log.debug("Mind rpc parsed sizes: head=" + ret[0] + " content=" + ret[1]);
		}
		return ret;
	}

	/**
	 * Reads a message headers from inputstream with a given length.
	 *
	 * @param inputStream the input stream
	 * @param length how much to read from a stream
	 * @return Map of headers
	 * @throws IOException if error occured
	 */
	protected Map<String, String> readHeaders(InputStream inputStream, int length) throws IOException {
		Map<String, String> map = new HashMap<String, String>();

		byte[] bytes = readBytes(inputStream, length);
		BufferedReader reader = new BufferedReader(new StringReader(new String(bytes)));
		String line;
		while ((line = reader.readLine()) != null) {
			if(log.isDebugEnabled()) {
				log.debug("deserialize header: " + line);
			}
			String[] split = line.split(":");
			if(split != null & split.length == 2) {
				map.put(split[0], split[1]);
			}
		}
		return map;
	}

	/**
	 * Helper method to read bytes from a stream.
	 *
	 * @param inputStream the input stream
	 * @param length how much to read
	 * @return bytes to read
	 * @throws IOException if error occured
	 */
	protected byte[] readBytes(InputStream inputStream, int length) throws IOException {
		byte[] buffer = new byte[length];
		int lengthRead = 0;
		while (lengthRead < length) {
			int len;
			len = inputStream.read(buffer, lengthRead, length - lengthRead);
			if (len < 0) {
				throw new IOException("Stream closed after " + lengthRead + " of " + length);
			}
			lengthRead += len;
		}
		return buffer;
	}

	protected void checkClosure(int bite) throws IOException {
		if (bite < 0) {
			if(log.isDebugEnabled()) {
				log.debug("Socket closed during message assembly");
			}
			throw new IOException("Socket closed during message assembly");
		}
	}

}
