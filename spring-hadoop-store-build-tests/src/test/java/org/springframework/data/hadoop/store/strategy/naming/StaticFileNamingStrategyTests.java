/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.hadoop.store.strategy.naming;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Tests for {@link StaticFileNamingStrategy}.
 *
 * @author Janne Valkealahti
 *
 */
public class StaticFileNamingStrategyTests {

	@Test
	public void testResolveNullPath() {
		StaticFileNamingStrategy strategy = new StaticFileNamingStrategy();
		assertThat(strategy.resolve(null).toString(), is("data"));
		strategy = new StaticFileNamingStrategy("custom");
		assertThat(strategy.resolve(null).toString(), is("custom"));
	}

	@Test
	public void testInit() {
		StaticFileNamingStrategy strategy = new StaticFileNamingStrategy();
		assertThat(strategy.init(new Path("/foo/jee")).toString(), is("/foo/jee"));
		assertThat(strategy.init(new Path("/foo/datajee")).toString(), is("/foo/jee"));
	}

	@Test
	public void testResolveWithGivenPath() {
		StaticFileNamingStrategy strategy = new StaticFileNamingStrategy();
		assertThat(strategy.resolve(new Path("/foo/jee")).toString(), is("/foo/jeedata"));
		strategy = new StaticFileNamingStrategy("custom");
		assertThat(strategy.resolve(new Path("/foo/jee")).toString(), is("/foo/jeecustom"));
	}

	@Test
	public void testEnabled() {
		StaticFileNamingStrategy strategy = new StaticFileNamingStrategy();
		assertThat(strategy.isEnabled(), is(true));
		strategy.setEnabled(false);
		assertThat(strategy.isEnabled(), is(false));
		assertThat(strategy.resolve(new Path("/foo/jee")).toString(), is("/foo/jee"));
	}

}
