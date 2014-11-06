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
package org.springframework.data.hadoop.pig;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ReflectionUtils;

import static org.junit.Assert.*;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/org/springframework/data/hadoop/pig/basic.xml")
public class PigTest {

	@Autowired
	private PigOperations pigTemplate;

	@Autowired
	private ApplicationContext ctx;

	@Test
	public void testPig() throws Exception {
		pigTemplate.executeScript("A = LOAD 'foo.txt' AS (key, value);");
	}

	@Test
	public void testServerNamespace() throws Exception {
		String defaultName = "pigFactory";
		assertTrue(ctx.containsBean(defaultName));
		PigServer server = (ctx.getBean(defaultName, PigServerFactory.class)).getPigServer();
		Properties props = server.getPigContext().getProperties();
		assertEquals("blue", props.get("ivy"));
		//assertEquals(ExecType.LOCAL, server.getPigContext().getExecType());
	}

	@Test
	public void testPigProperties() throws Exception {
		PigServer pig = (ctx.getBean("pigFactory", PigServerFactory.class)).getPigServer();
		Properties props = pig.getPigContext().getProperties();
		assertEquals("blue", props.get("ivy"));

		assertEquals("chasing", props.get("star"));
		assertEquals("captain eo", props.get("return"));
		assertEquals("last", props.get("train"));
		assertEquals("the dream", props.get("dancing"));
		assertEquals("in the mirror", props.get("tears"));
		assertEquals("eo", props.get("captain"));
	}

	@Test
	public void testPigScriptOrdering() throws Exception {
		PigServerFactoryBean psfb = (PigServerFactoryBean) ctx.getBean("&pigFactory");
		Field findField = ReflectionUtils.findField(PigServerFactoryBean.class, "scripts");
		ReflectionUtils.makeAccessible(findField);

		@SuppressWarnings("unchecked")
		Collection<PigScript> scripts = (Collection<PigScript>) ReflectionUtils.getField(findField, psfb);
		assertEquals(1, scripts.size());
		PigScript firstScript = scripts.iterator().next();
		Map<String, String> args = firstScript.getArguments();
		Iterator<String> keys = args.keySet().iterator();
		assertEquals("war", keys.next());
		assertEquals("blue", keys.next());
		assertEquals("white", keys.next());
	}

	@Test
	public void testPigRunner() throws Exception {
		@SuppressWarnings("unchecked")
		List<ExecJob> jobs = ctx.getBean("pig-scripts", List.class);
		System.out.println(jobs.size());
	}

	@Test
	public void testPigTemplate() throws Exception {
		System.out.println(pigTemplate.execute(new PigCallback<Object>() {
			@Override
			public Object doInPig(PigServer pig) throws ExecException, IOException {
				return pig.getAliasKeySet();
			}
		}));
	}
}