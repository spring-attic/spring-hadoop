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
package org.springframework.data.hadoop.cascading;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import cascading.cascade.Cascade;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowConnectorProps;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.DebugLevel;
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
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CascadingTest {

	{
		TestUtils.hackHadoopStagingOnWin();
	}

	@Autowired
	private ApplicationContext ctx;
	@Autowired
	private Cascade cascade;
	@Autowired
	private Configuration hadoopConfiguration;

	@Test
	public void testCascade() throws Exception {
		List<Flow> flows = cascade.getFlows();
		System.out.println(flows);
	}

	//@Test
	// disable this for now
	public void testManualCascade() throws Exception {
		ctx.getBean("script");

		Scheme sourceScheme = new TextDelimited(new Fields("name", "definition"), ",");
		Tap source = new Hfs(sourceScheme, "/test/cascading/names/input/babynamedefinitions.csv.gz");

		Scheme sinkScheme = new TextDelimited(new Fields("definition", "name"), " $$ ");
		Tap sink = new Hfs(sinkScheme, "/test/cascading/names/output/simplepipe", SinkMode.REPLACE);

		Pipe assembly = new Pipe("flip");
		//OPTIONAL: Debug the tuple
		//assembly = new Each(assembly, DebugLevel.VERBOSE, new Debug());

		// wire the existing Hadoop config into HadoopFlow
		Configuration cfg = ConfigurationUtils.createFrom(hadoopConfiguration, null);
		//Resource cascadeCore = ResourceUtils.findContainingJar(Cascade.class);
		// find cascade-hadoop
		// Resource cascadeHadoop = ResourceUtils.findContainingJar(HadoopFlow.class);

		//ConfigurationUtils.addLibs(cfg, cascadeHadoop);
		//ConfigurationUtils.addArchives(cfg, cascadeHadoop);
		// ConfigurationUtils.addFiles(cfg, cascadeHadoop);

		//cfg.set("mapred.jar", cascadeHadoop.getURL().toString());

		Properties props = ConfigurationUtils.asProperties(cfg);
		System.out.println(props);

		FlowConnector flowConnector = new HadoopFlowConnector(props);

		FlowConnectorProps.setDebugLevel(props, DebugLevel.VERBOSE);
		Flow flow = flowConnector.connect("flipflow", source, sink, assembly);
		flow.complete();
	}
}
