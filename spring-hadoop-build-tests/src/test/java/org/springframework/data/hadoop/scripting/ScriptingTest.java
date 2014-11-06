/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.hadoop.scripting;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.UrlResource;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.data.hadoop.fs.SimplerFileSystem;
import org.springframework.scripting.ScriptSource;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.scripting.support.StaticScriptSource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class ScriptingTest {

	@Resource
	private ApplicationContext ctx;
	@Resource
	private Configuration config;

	@Test
	public void testRhinoScript() throws Exception {
		ScriptSource script = new StaticScriptSource("msg = 'Hello, world!'");

		Jsr223ScriptEvaluator eval = new Jsr223ScriptEvaluator();
		eval.setLanguage("javascript");

		assertEquals("Hello, world!", eval.evaluate(script));
	}

	@Test
	public void testRhinoHadoopScript() throws Exception {
		UrlResource urlResource = new UrlResource(getClass().getResource("basic-script.js"));
		ScriptSource script = new ResourceScriptSource(urlResource);

		Jsr223ScriptEvaluator eval = new Jsr223ScriptEvaluator();
		eval.setLanguage("javascript");

		Map<String, Object> args = new LinkedHashMap<String, Object>();
		FileSystem fs = FileSystem.get(config);
		SimplerFileSystem sfs = new SimplerFileSystem(fs);
		
		args.put("fs", sfs);
		args.put("fsh", new FsShell(config, sfs));

		eval.evaluate(script, args);
	}

	@Test
	public void testScriptNSJavaScript() throws Exception {
		assertTrue(ctx.containsBean("script-js"));
		assertNotNull(ctx.getBean("script-js"));
	}

	@Test
	public void testScriptNSJython() throws Exception {
		assertTrue(ctx.containsBean("script-py"));
		ctx.getBean("script-py");
	}

	@Test
	public void testScriptNSJRuby() throws Exception {
		assertTrue(ctx.containsBean("script-rb"));
		assertNotNull(ctx.getBean("script-rb"));
	}

	@Test
	public void testScriptNSJGroovy() throws Exception {
		assertTrue(ctx.containsBean("script-groovy"));
		assertNotNull(ctx.getBean("script-groovy"));
	}

	@Test
	public void testInlinedScriptNSJavaScript() throws Exception {
		assertTrue(ctx.containsBean("inlined-js"));
		assertNotNull(ctx.getBean("inlined-js"));
	}

	@Test
	public void testDistCp() throws Exception {
		assertTrue(ctx.containsBean("distcp"));
		ctx.getBean("distcp");
	}

	@Test
	public void testNullCfg() throws Exception {
		ScriptSource script = new StaticScriptSource("null");
		
		HdfsScriptRunner hsfb = new HdfsScriptRunner();
		GenericApplicationContext gac = new GenericApplicationContext();
		gac.refresh();
		hsfb.setApplicationContext(gac);
		hsfb.setScriptSource(script);
		hsfb.setLanguage("javascript");
		hsfb.afterPropertiesSet();
		
		assertNull(hsfb.call());
	}
}