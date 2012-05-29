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
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

/**
 * Aggregation Resource allowing multiple resources to be used as one. 
 * 
 * @author Costin Leau
 */
class DelegatingResource extends AbstractResource {

	private final Resource[] resources;
	private final boolean exists, readable, open;
	private final long lastModified, contentLength;
	private final String toString;

	DelegatingResource(Resource[] resources) {
		Assert.notEmpty(resources, "at least one resource needs to be specified");
		this.resources = resources;

		boolean e = false;
		boolean r = false;
		boolean o = false;
		long lm = -1;
		long cl = 0;
		for (Resource resource : resources) {
			e |= resource.exists();
			r |= resource.isReadable();
			o |= resource.isOpen();
			try {
				long llm = resource.lastModified();
				if (llm > lm) {
					lm = llm;
				}
			} catch (IOException ex) {
				// ignore
			}

			try {
				cl += resource.contentLength();
			} catch (IOException ex) {
				// ignore
			}
		}

		exists = e;
		readable = r;
		open = o;
		lastModified = lm;
		contentLength = cl;
		toString = "DelegatingResource for " + Arrays.toString(resources);
	}

	@Override
	public InputStream getInputStream() throws IOException {
		Collection<InputStream> streams = new ArrayList<InputStream>(resources.length);
		for (Resource res : resources) {
			streams.add(res.getInputStream());
		}
		return new SequenceInputStream(Collections.enumeration(streams));
	}

	@Override
	public boolean exists() {
		return exists;
	}

	@Override
	public boolean isReadable() {
		return readable;
	}

	@Override
	public boolean isOpen() {
		return open;
	}

	@Override
	public long contentLength() throws IOException {
		return contentLength;
	}

	@Override
	public long lastModified() throws IOException {
		return lastModified;
	}

	@Override
	public String getDescription() {
		return toString;
	}
}