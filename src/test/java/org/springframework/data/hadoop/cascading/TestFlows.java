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
package org.springframework.data.hadoop.cascading;

import cascading.flow.FlowDef;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * @author Costin Leau
 */
public class TestFlows {

	public static FlowDef copyFlow(String sourcePath, String sinkPath) {
		Scheme sourceScheme = new TextDelimited(new Fields("name", "definition"), ",");

		Tap source = new Hfs(sourceScheme, sourcePath);

		Scheme sinkScheme = new TextDelimited(new Fields("definition", "name"), " $$ ");
		Tap sink = new Hfs(sinkScheme, sinkPath, SinkMode.REPLACE);

		Pipe assembly = new Pipe("copy");
		assembly = new Each(assembly, DebugLevel.VERBOSE, new Debug());

		FlowDef flowDef = FlowDef.flowDef().setName("copy");
		flowDef.addTail(assembly);
		flowDef.addSource("copy", source);
		flowDef.addSink("copy", sink);
		flowDef.setDebugLevel(DebugLevel.VERBOSE);
		return flowDef;
	}
}
