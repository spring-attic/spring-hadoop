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
import java.io.InputStream;
import java.util.Properties;

import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SourceTap;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIterator;

/**
 * {@link SourceTap} for Spring's {@link Resource} abstraction. 
 * Multiple resources (resulting from pattern matching for example) can be used. 
 * 
 * @author Costin Leau
 */
@SuppressWarnings("serial")
public class ResourceTap extends SourceTap<Properties, Resource> {

	private final Resource resource;
	
	/**
	 * Constructs a new <code>ResourceTap</code> instance.
	 *
	 * @param scheme scheme to use
	 * @param resource Resource to read from
	 */
	public ResourceTap(Scheme<Properties, Resource, ?, ?, ?> scheme, Resource resource) {
		this(scheme, new Resource[] { resource });
	}

	/**
	 * Constructs a new <code>ResourceTap</code> instance. Backed by multiple resources (
	 * allowing for pattern matching to occur).
	 *
	 * @param scheme scheme to use
	 * @param resources Resources to read from
	 */
	public ResourceTap(Scheme<Properties, Resource, ?, ?, ?> scheme, Resource[] resources) {
		super(scheme);
		Assert.notEmpty(resources, "at least one resource is required");
		this.resource = (resources.length == 1 ? resources[0] : new DelegatingResource(resources));
	}

	@Override
	public String getIdentifier() {
		return resource.toString();
	}

	@Override
	public TupleEntryIterator openForRead(FlowProcess<Properties> flowProcess, Resource input) throws IOException {
		InputStream in = (input != null ? input.getInputStream() : resource.getInputStream());
		return new TupleEntrySchemeIterator<Properties, InputStream>(flowProcess, getScheme(), in, getIdentifier());
	}

	@Override
	public boolean resourceExists(Properties conf) throws IOException {
		return resource.exists();
	}

	@Override
	public long getModifiedTime(Properties conf) throws IOException {
		return resource.lastModified();
	}
}