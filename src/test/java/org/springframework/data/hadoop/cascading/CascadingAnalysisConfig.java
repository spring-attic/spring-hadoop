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

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import cascading.cascade.Cascades;
import cascading.operation.aggregator.Count;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * @author Costin Leau
 */
@Configuration
public class CascadingAnalysisConfig {

	@Value("${cascade.sec}") private String sec;
	@Value("${cascade.min}") private String min;
	
	@Bean public Pipe tsPipe() {
		DateParser dateParser = new DateParser(new Fields("ts"), "dd/MMM/yyyy:HH:mm:ss Z");
		return new Each("arrival rate", new Fields("time"), dateParser);
	}

	@Bean public Pipe tsCountPipe() {
		Pipe tsCountPipe = new Pipe("tsCount", tsPipe());
		tsCountPipe = new GroupBy(tsCountPipe, new Fields("ts"));
		return new Every(tsCountPipe, Fields.GROUP, new Count());
	}

	@Bean public Pipe tmCountPipe() {
		Pipe tmPipe = new Each(tsPipe(),
				new ExpressionFunction(new Fields("tm"), "ts - (ts % (60 * 1000))", long.class));
		Pipe tmCountPipe = new Pipe("tmCount", tmPipe);
		tmCountPipe = new GroupBy(tmCountPipe, new Fields("tm"));
		return new Every(tmCountPipe, Fields.GROUP, new Count());
	}

	@Bean public Map<String, Tap> sinks(){
		Tap tsSinkTap = new Hfs(new TextLine(), sec);
		Tap tmSinkTap = new Hfs(new TextLine(), min);
		return Cascades.tapsMap(Pipe.pipes(tsCountPipe(), tmCountPipe()), Tap.taps(tsSinkTap, tmSinkTap));
	}

	@Bean public String regex() {
		return "^([^ ]*) +[^ ]* +[^ ]* +\\[([^]]*)\\] +\\\"([^ ]*) ([^ ]*) [^ ]*\\\" ([^ ]*) ([^ ]*).*$";
	}

	@Bean public Fields fields() {
		return new Fields("ip", "time", "method", "event", "status", "size");
	}
}
