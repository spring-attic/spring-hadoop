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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Mind request and response contains a set of headers and a content. This is
 * similar to http traffict. This holder keeps those separated and makes it
 * easier to work with payloads.
 *
 * @author Janne Valkealahti
 *
 */
public class MindRpcMessageHolder {

	/** Map of headers */
	private Map<String, String> headers;

	/** Content of the holder */
	private byte[] content;

	/**
	 * Constructs holder instance with a map of headers and a content.
	 *
	 * @param headers the headers
	 * @param content the content
	 */
	public MindRpcMessageHolder(Map<String, String> headers, String content) {
		super();
		this.headers = headers != null ? headers : new HashMap<String, String>();
		this.content = content.getBytes();
	}

	/**
	 * Constructs holder instance with a map of headers and a content.
	 *
	 * @param headers the headers
	 * @param content the content
	 */
	public MindRpcMessageHolder(Map<String, String> headers, byte[] content) {
		super();
		this.headers = headers != null ? headers : new HashMap<String, String>();
		this.content = content;
	}

	/**
	 * Sets the headers of this holder. Overrides any
	 * previous headers.
	 *
	 * @param headers the headers
	 */
	public void setHeaders(Map<String, String> headers) {
		this.headers = headers;
	}

	/**
	 * Sets the content of this holder.
	 *
	 * @param content the content
	 */
	public void setContent(byte[] content) {
		this.content = content;
	}

	/**
	 * Sets the content of this holder.
	 *
	 * @param content the content
	 */
	public void setContent(String content) {
		this.content = content.getBytes();
	}

	/**
	 * Gets headers of this holder.
	 *
	 * @return the map of headers
	 */
	public Map<String, String> getHeaders() {
		return headers;
	}

	/**
	 * Gets the content of this holder.
	 *
	 * @return content of this holder.
	 */
	public byte[] getContent() {
		return content;
	}

	/**
	 * Gets the complete protocol representation of this
	 * holder as array of bytes. This array is then send
	 * over the channel.
	 *
	 * @return byte array of protocol message
	 */
	public byte[] toBytes() {
		StringBuilder buf = new StringBuilder();
		for (Entry<String, String> entry : headers.entrySet()) {
			buf.append(entry.getKey());
			buf.append(": ");
			buf.append(entry.getValue());
			buf.append("\r\n");
		}
		buf.append("\r\n");
		int l1 = buf.length();
		buf.insert(0, "MRPC/2 " + l1 + " " + content.length + "\r\n");
		byte[] bytes1 = buf.toString().getBytes();
		byte[] combined = new byte[content.length+bytes1.length];
		System.arraycopy(bytes1, 0, combined, 0, bytes1.length);
		System.arraycopy(content, 0, combined, bytes1.length, content.length);
		return combined;
	}

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append("Holder headers={");
		for(Entry<String, String> entry : headers.entrySet()) {
			buf.append(entry.getKey());
			buf.append("=");
			buf.append(entry.getValue());
			buf.append(",");
		}
		buf.append("} Content size=");
		buf.append(content != null ? content.length : 0);
		buf.append(", Content=");
		buf.append(content != null ? new String(content) : "");
		return buf.toString();
	}

}
