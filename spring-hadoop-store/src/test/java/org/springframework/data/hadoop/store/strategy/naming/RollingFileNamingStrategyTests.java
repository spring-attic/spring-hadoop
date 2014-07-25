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
import org.springframework.data.hadoop.store.TestUtils;

/**
 * Tests for {@link RollingFileNamingStrategy}.
 *
 * @author Janne Valkealahti
 *
 */
public class RollingFileNamingStrategyTests {

	@Test
	public void testResolveNullPath() {
		RollingFileNamingStrategy strategy = new RollingFileNamingStrategy();
		assertThat(strategy.resolve(null).toString(), is("0"));
	}

	@Test
	public void testInit() throws Exception {
		RollingFileNamingStrategy strategy = new RollingFileNamingStrategy();
		assertThat(strategy.init(new Path("/foo/jee")).toString(), is("/foo/jee"));
		assertThat((Integer)TestUtils.readField("counter", strategy), is(0));
		assertThat(strategy.init(new Path("/foo/jee-0")).toString(), is("/foo/jee"));
		assertThat((Integer)TestUtils.readField("counter", strategy), is(1));
		assertThat(strategy.init(new Path("/foo/jee-1")).toString(), is("/foo/jee"));
		assertThat((Integer)TestUtils.readField("counter", strategy), is(2));
		assertThat(strategy.init(new Path("/foo/jee-12345")).toString(), is("/foo/jee"));
		assertThat((Integer)TestUtils.readField("counter", strategy), is(12346));
		assertThat(strategy.init(new Path("/foo/jee-1-0")).toString(), is("/foo/jee-0"));
		assertThat((Integer)TestUtils.readField("counter", strategy), is(1));
	}

	@Test
	public void testResolveWithGivenPath() {
		RollingFileNamingStrategy strategy = new RollingFileNamingStrategy();
		assertThat(strategy.resolve(new Path("/foo/jee")).toString(), is("/foo/jee-0"));
		strategy.next();
		assertThat(strategy.resolve(new Path("/foo/jee")).toString(), is("/foo/jee-1"));
		strategy.next();
		assertThat(strategy.resolve(new Path("/foo/jee")).toString(), is("/foo/jee-2"));
	}

	@Test
	public void testEnabled() {
		RollingFileNamingStrategy strategy = new RollingFileNamingStrategy();
		assertThat(strategy.isEnabled(), is(true));
		strategy.setEnabled(false);
		assertThat(strategy.isEnabled(), is(false));
		assertThat(strategy.resolve(new Path("/foo/jee")).toString(), is("/foo/jee"));
		strategy.setEnabled(true);
		assertThat(strategy.resolve(new Path("/foo/jee")).toString(), is("/foo/jee-0"));
	}

	@Test
	public void testInitPaths() throws Exception {
		RollingFileNamingStrategy strategy = new RollingFileNamingStrategy();
		strategy.init(new Path("/foo/jee-123.txt"));
		Integer counter = TestUtils.readField("counter", strategy);
		assertThat(counter, is(124));
		strategy.init(new Path("/projects/loganalysis/yarn/2014/07/24/00/logIngestion-98e327c1-5f24-463a-88d7-571195529825-0.txt"));
		counter = TestUtils.readField("counter", strategy);
		assertThat(counter, is(1));
	}

}
