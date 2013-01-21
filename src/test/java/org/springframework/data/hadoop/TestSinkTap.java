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
package org.springframework.data.hadoop;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.local.StdOutTap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntrySchemeCollector;

/**
 * @author Costin Leau
 */
public class TestSinkTap extends StdOutTap {

	private final OutputStream stream;

	public TestSinkTap(Scheme<Properties, ?, OutputStream, ?, ?> scheme, OutputStream stream) {
		super(scheme);
		this.stream = stream;
	}

	@Override
	public TupleEntryCollector openForWrite(FlowProcess<Properties> flowProcess, OutputStream output)
			throws IOException {
		return new TupleEntrySchemeCollector<Properties, OutputStream>(flowProcess, getScheme(), stream);
	}
}
